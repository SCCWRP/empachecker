import os, time, re, json, random
import pandas as pd
import geopandas as gpd
from bs4 import BeautifulSoup
from flask import Blueprint, g, current_app, render_template, redirect, url_for, session, request, jsonify, render_template_string, send_file
from io import StringIO
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, text
import io
import tempfile
import zipfile

from .utils.db import metadata_summary
from .utils.route_auth import requires_auth
from .utils.mail import send_mail

admin = Blueprint('admin', __name__)

@admin.route('/admin', methods=['GET'])
def admin_portal():
    """Admin portal - no password required"""
    return render_template('admin_portal.html', authorized=True)

@admin.route('/track')
def tracking():
    print("start track")
    sql_session =   '''
                    SELECT LOGIN_EMAIL,
                        LOGIN_AGENCY,
                        SUBMISSIONID,
                        DATATYPE,
                        SUBMIT,
                        CREATED_DATE,
                        ORIGINAL_FILENAME
                    FROM SUBMISSION_TRACKING_TABLE
                    WHERE SUBMISSIONID IS NOT NULL
                        AND ORIGINAL_FILENAME IS NOT NULL
                    ORDER BY CREATED_DATE DESC
                    '''
    with g.eng.connect() as conn:
        session_results = conn.execute(text(sql_session))
        session_json = [dict(r._mapping) for r in session_results]
    authorized = session.get("AUTHORIZED_FOR_ADMIN_FUNCTIONS")
    
    # session is a reserved word in flask - renaming to something different
    return render_template('track.html', session_json=session_json, authorized=authorized)


@admin.route('/schema')
def schema():
    print("entering schema")

    sessionid = int(time.time())
    dl_filename = f"{sessionid}.xlsx"

    authorized = session.get("AUTHORIZED_FOR_ADMIN_FUNCTIONS")

    print("start schema information lookup routine")
    eng = g.eng
    datatype = request.args.get("datatype")
    
    if datatype is not None:
        if datatype not in current_app.datasets.keys():
            return f"Datatype {datatype} not found"

        # dictionary to return
        return_object = {}
        
        tables = current_app.datasets.get(datatype).get("tables")
        print("tables")
        print(tables)
        for tbl in tables:
            print("tbl: ")
            print(tbl)
            df = metadata_summary(tbl, eng)
            
            
            df['lookuplist_table_name'] = df['lookuplist_table_name'].apply(
                lambda x: f"""<a target=_blank href=/{current_app.script_root}/scraper?action=help&layer={x}>{x}</a>""" if pd.notnull(x) else ''
            )

            # drop "table_name" column
            print("# drop table_name column")
            df.drop('tablename', axis = 'columns', inplace = True)

            # drop system fields
            print("# drop system fields")
            df.drop(df[df.column_name.isin(current_app.system_fields)].index, axis = 'rows', inplace = True)

            print("df fill na")
            df.fillna('', inplace = True)
            print("before return obj")

            

            return_object[tbl] = df.to_dict('records')
        
        
        print(" before with ")
        with pd.ExcelWriter(os.path.join(os.getcwd(), "export", dl_filename)) as writer:
            for key in return_object.keys():
                df_to_download = pd.DataFrame.from_dict(return_object[key])
                df_to_download['lookuplist_table_name'] = df_to_download['lookuplist_table_name'].apply(
                    lambda x: "https://{}/{}/scraper?action=help&layer={}".format(
                        request.host,
                        current_app.config.get('APP_SCRIPT_ROOT'),
                        BeautifulSoup(x, 'html.parser').text.strip()
                    ) if BeautifulSoup(x, 'html.parser').text.strip() != '' else ''
                )
                df_to_download.to_excel(writer, sheet_name=key, index=False)

        # print("return_object")
        # print(return_object)

        return render_template('schema.html', metadata=return_object, datatype=datatype, authorized=authorized, dl_filename=dl_filename)
        
    # only executes if "datatypes" not given
    datatypes = current_app.datasets
    return render_template('schema.html', datatypes=datatypes, authorized=authorized, dl_filename=dl_filename)

@admin.route('/column-order', methods = ['GET','POST'])
def column_order():
    authorized = session.get("AUTHORIZED_FOR_ADMIN_FUNCTIONS")
    if not authorized:
        # return template for GET request, empty string for everything else
        return render_template('admin_password.html', redirect_route='column-order') \
            if request.method == 'GET' \
            else ''
    

    # connect with psycopg2
    connection = psycopg2.connect(
        host=os.environ.get("DB_HOST"),
        database=os.environ.get("DB_NAME"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("PGPASSWORD"),
    )

    connection.set_session(autocommit=True)

    if request.method == 'GET':
        eng = g.eng

        # update column-order table based on contents of information schema
        cols_to_add_qry = (
            """
            WITH cols_to_add AS (
                SELECT 
                    table_name,
                    column_name,
                    ordinal_position AS original_db_position,
                    ordinal_position AS custom_column_position 
                FROM
                    information_schema.COLUMNS 
                WHERE
                    table_name IN ( SELECT DISTINCT table_name FROM column_order ) 
                    AND ( table_name, column_name ) NOT IN ( SELECT DISTINCT table_name, column_name FROM column_order )
            )
            INSERT INTO 
                column_order (table_name, column_name, original_db_position, custom_column_position) 
                (
                    SELECT table_name, column_name, original_db_position, custom_column_position FROM cols_to_add
                )
            ;
            """
        )

        # remove records from column order if they are not there anymore
        cols_to_delete_qry = (
            """
            WITH cols_to_delete AS (
                SELECT TABLE_NAME
                    ,
                    COLUMN_NAME,
                    original_db_position,
                    custom_column_position 
                FROM
                    column_order 
                WHERE
                    TABLE_NAME NOT IN ( SELECT DISTINCT TABLE_NAME FROM information_schema.COLUMNS ) 
                    OR ( TABLE_NAME, COLUMN_NAME ) NOT IN ( SELECT DISTINCT TABLE_NAME, COLUMN_NAME FROM information_schema.COLUMNS ) 
                ) 
                DELETE FROM column_order 
                WHERE
                    ( TABLE_NAME, COLUMN_NAME ) IN ( SELECT TABLE_NAME, COLUMN_NAME FROM cols_to_delete );
            ;
            """
        )
        with connection.cursor() as cursor:
            command = sql.SQL(cols_to_add_qry)
            cursor.execute(command)
            command = sql.SQL(cols_to_delete_qry)
            cursor.execute(command)

        basequery = (
            """
            WITH baseqry AS (
                SELECT table_name, column_name, custom_column_position FROM column_order ORDER BY table_name, custom_column_position
            )
            SELECT * FROM baseqry
            """
        )
        
        # Query string arg to get the specific datatype
        datatype = request.args.get("datatype")
        
        # If a specific datatype is selected then display the schema for it
        if datatype is not None:
            if datatype not in current_app.datasets.keys():
                return f"Datatype {datatype} not found"

            # dictionary to return
            return_object = {}
            
            tables = current_app.datasets.get(datatype).get("tables")
            for tbl in tables:
                with eng.connect() as conn:
                    df = pd.read_sql(f"{basequery} WHERE table_name = '{tbl}';", conn)

                df.fillna('', inplace = True)

                return_object[tbl] = df.to_dict('records')
            
            # Return the datatype query string arg - the template will need access to that
            return render_template('column-order.jinja2', metadata=return_object, datatype=datatype, authorized=authorized)
        
        # only executes if "datatypes" not given
        datatypes_list = current_app.datasets.keys()
        return render_template('column-order.jinja2', datatypes_list=datatypes_list, authorized=authorized)
        
    elif request.method == 'POST':
        try:
            data = request.get_json()

            tablename = str(data.get("tablename")).strip()
            column_order_information = data.get("column_order_information")

            with connection.cursor() as cursor:
                for item in column_order_information:
                    column_name = item.get('column_name')
                    column_position = item.get('column_position')
                    command = sql.SQL(
                        """
                        UPDATE column_order 
                            SET custom_column_position = {pos} 
                        WHERE 
                            column_order.table_name = {tablename} 
                            AND column_order.column_name = {column_name};
                        """
                    ).format(
                        pos = sql.Literal(column_position),
                        tablename = sql.Literal(tablename),
                        column_name = sql.Literal(column_name)
                    )
                    
                    cursor.execute(command)

            connection.close()
            return jsonify(message=f"Successfully updated column order for {tablename}")
        except Exception as e:
            print(e)
            return jsonify(message=f"Error: {str(e)}")

    else:
        return ''

    

@admin.route('/adminauth', methods = ['POST'])
def adminauth():
    # I put a link in the schema page for some who want to edit the schema to sign in
    # I put schema as as query string arg to show i want them to be redirected there after they sign in
    if request.args.get("redirect_to"):
        return render_template('admin_password.html', redirect_route=request.args.get("redirect_to"))
    
    adminpw = request.get_json().get('adminpw')

    if adminpw == os.environ.get("ADMIN_FUNCTION_PASSWORD"):
        session['AUTHORIZED_FOR_ADMIN_FUNCTIONS'] = True
    else:
        session['AUTHORIZED_FOR_ADMIN_FUNCTIONS'] = False
    print("session['AUTHORIZED_FOR_ADMIN_FUNCTIONS']")
    print(session.get("AUTHORIZED_FOR_ADMIN_FUNCTIONS"))
    return jsonify(message=str(session.get("AUTHORIZED_FOR_ADMIN_FUNCTIONS")).lower())


@admin.route('/update_column_description', methods = ['POST'])
def update_column_description():

    data = request.get_json()
    
    # I am trying to prevent sql injection - Duy
    table_name = re.sub(r"[^\w\s']", '', data.get('table_name').strip()) 
    field_name = re.sub(r"[^\w\s']", '', data.get('field_name').strip())
    new_description = re.sub(r"[^\w\s']", '', data.get('new_description').strip())

    update_query = f"COMMENT ON COLUMN {table_name}.{field_name} IS '{new_description}';"

    # connect with psycopg2
    connection = psycopg2.connect(
        host=os.environ.get("DB_HOST"),
        database=os.environ.get("DB_NAME"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("PGPASSWORD"),
    )

    connection.set_session(autocommit=True)

    with connection.cursor() as cursor:
        command = sql.SQL(update_query)
        cursor.execute(command)

    return jsonify(message="updated successfully")



@admin.route('/inventory', methods=['GET', 'POST'])
# @requires_auth
def report():
    return render_template("inventory-main.html")

@admin.route('/report-download', methods=['GET', 'POST'])
def report_download():
    # Return as a downloadable file
    return send_file(os.path.join(os.getcwd(), 'export','inventory-report.csv'), as_attachment=True, download_name="report.csv", mimetype="text/csv")


@admin.route('/get-inventory-data', methods=['GET'])
def get_inventory_data():
    eng = create_engine(os.environ.get('DB_CONNECTION_STRING_READONLY'))

    with eng.connect() as conn:
        # Query data for the Logger using Pandas
        logger_query = "SELECT * FROM mvw_logger_inventory"
        logger_df = pd.read_sql(logger_query, con=conn)
        logger_df['year'] = logger_df['year'].astype(int).astype(str)

        # Query data for the General using Pandas
        general_query = \
            """
                SELECT 
                    *
                FROM
                    vw_data_inventory
            """
        general_df = pd.read_sql(general_query, con=conn)
        general_df['year'] = general_df['year'].astype(int).astype(str)

    # Prepare the data structure
    inventory_data = {
        'general': {
            'minYear': general_df['year'].min(),
            'maxYear': general_df['year'].max(),
            'data': {}
        },
        'logger': {
            'minYear': logger_df['year'].min(),
            'maxYear': logger_df['year'].max(),
            'data': {}
        }
    }

    # Populate general data from DataFrame
    for _, row in general_df.iterrows():
        sop = f'sop{row["sop"]}'
        siteid = row['siteid']
        year = str(row['year'])
        season = row['season']
        data_exists = row['data_exists']

        if sop not in inventory_data['general']['data']:
            inventory_data['general']['data'][sop] = {}

        if siteid not in inventory_data['general']['data'][sop]:
            inventory_data['general']['data'][sop][siteid] = {}

        if year not in inventory_data['general']['data'][sop][siteid]:
            inventory_data['general']['data'][sop][siteid][year] = {}

        inventory_data['general']['data'][sop][siteid][year][season] = data_exists

    # Populate logger data from DataFrame
    parameter_columns = logger_df.columns.difference(['region', 'siteid', 'year', 'month'])  # Identify parameter columns

    for _, row in logger_df.iterrows():
        siteid = row['siteid']
        year = str(row['year'])
        month = str(row['month'])

        for parameter in parameter_columns:
            data_exists = row[parameter]

            if parameter not in inventory_data['logger']['data']:
                inventory_data['logger']['data'][parameter] = {}

            if siteid not in inventory_data['logger']['data'][parameter]:
                inventory_data['logger']['data'][parameter][siteid] = {}

            if year not in inventory_data['logger']['data'][parameter][siteid]:
                inventory_data['logger']['data'][parameter][siteid][year] = {}

            inventory_data['logger']['data'][parameter][siteid][year][month] = data_exists

    # Process SOP 1 (logger) data to match general SOP structure
    # Group by siteid, year, season and check if any raw_ column has 'y'
    raw_columns = [col for col in logger_df.columns if col.startswith('raw_')]

    # Define season based on month
    def get_season(month):
        month = int(month)
        if month in [3, 4, 5, 6]:
            return 'Spring'
        elif month in [7, 8, 9, 10, 11, 12, 1, 2]:
            return 'Fall'
        return None

    # Add season column
    logger_df['season'] = logger_df['month'].apply(get_season)

    # Group by siteid, year, season and check if any raw_ column has 'y'
    sop1_grouped = logger_df.groupby(['siteid', 'year', 'season']).apply(
        lambda x: 'Data Available' if any(x[col].eq('y').any() for col in raw_columns) else 'Not Submitted'
    ).reset_index(name='data_exists')

    # Add SOP 1 to general data structure
    if 'sop1' not in inventory_data['general']['data']:
        inventory_data['general']['data']['sop1'] = {}

    for _, row in sop1_grouped.iterrows():
        siteid = row['siteid']
        year = str(row['year'])
        season = row['season']
        data_exists = row['data_exists']

        if siteid not in inventory_data['general']['data']['sop1']:
            inventory_data['general']['data']['sop1'][siteid] = {}

        if year not in inventory_data['general']['data']['sop1'][siteid]:
            inventory_data['general']['data']['sop1'][siteid][year] = {}

        inventory_data['general']['data']['sop1'][siteid][year][season] = data_exists

    return jsonify(inventory_data)

@admin.route('/download-inventory-data', methods=['GET'])
def download_inventory_data():
    eng = create_engine(os.environ.get('DB_CONNECTION_STRING_READONLY'))

    # Query data for the General using Pandas
    general_query = """
        SELECT 
            sop,region,siteid,year,season,data_exists,months_with_data
        FROM
            vw_data_inventory 
        ORDER BY
            sop,
            region,
            siteid,
            year
    """

    with eng.connect() as conn:
        general_df = pd.read_sql(general_query, con=conn)

    # Filter by year if provided
    year_param = request.args.get('year')
    if year_param:
        general_df = general_df[general_df['year'] == int(year_param)]

    # SOP code to long name mapping
    sop_code_longname = {
        "field": "Field Grab",
        "2": "SOP 2: Discrete environmental monitoring - point water quality measurements",
        "3a": "SOP 3: Sediment chemistry",
        "3b": "SOP 3: Sediment toxicity",
        "4": "SOP 4: eDNA - field",
        "5": "SOP 5: Sediment grain size analysis",
        "6a": "SOP 6: Benthic infauna, small",
        "6b": "SOP 6: Benthic infauna, large",
        "7": "SOP 7: Macroalgae",
        "8a": "SOP 8: Fish - BRUVs - Field",
        "8b": "SOP 8: Fish - BRUVs - Lab",
        "9": "SOP 9: Fish seines",
        "10": "SOP 10: Crab traps",
        "11": "SOP 11: Marsh plain vegetation and epifauna surveys",
        "12": "SOP 12: Topographic survey",
        "13": "SOP 13: Sediment accretion rates",
        "15": "SOP 15: Trash monitoring"
    }

    # Map SOP codes to long names
    general_df['sop'] = general_df['sop'].map(sop_code_longname)

    # Ensure 'season' is ordered as Spring, Fall
    season_cat = pd.CategoricalDtype(['Spring', 'Fall'], ordered=True)
    general_df['season'] = general_df['season'].astype(season_cat)

    # Pivot the DataFrame to wide format
    pivot_df = general_df.pivot_table(
        index=['siteid', 'year', 'season'],
        columns='sop',
        values='data_exists',
        aggfunc='first'  # In case of duplicates, take the first
    ).reset_index().sort_values(['siteid', 'year', 'season'])

    # Flatten columns if needed
    pivot_df.columns.name = None

    # Desired SOP columns in order (long names)
    sop_columns = [
        "Field Grab",
        "SOP 2: Discrete environmental monitoring - point water quality measurements",
        "SOP 3: Sediment chemistry",
        "SOP 3: Sediment toxicity",
        "SOP 4: eDNA - field",
        "SOP 5: Sediment grain size analysis",
        "SOP 6: Benthic infauna, small",
        "SOP 6: Benthic infauna, large",
        "SOP 7: Macroalgae",
        "SOP 8: Fish - BRUVs - Field",
        "SOP 8: Fish - BRUVs - Lab",
        "SOP 9: Fish seines",
        "SOP 10: Crab traps",
        "SOP 11: Marsh plain vegetation and epifauna surveys",
        "SOP 12: Topographic survey",
        "SOP 13: Sediment accretion rates",
        "SOP 15: Trash monitoring"
    ]
    # Ensure all columns exist
    for col in sop_columns:
        if col not in pivot_df.columns:
            pivot_df[col] = ''

    # Reorder columns
    ordered_cols = ['siteid', 'year', 'season'] + sop_columns
    pivot_df = pivot_df[ordered_cols]

    # Write to Excel with conditional formatting
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        pivot_df.to_excel(writer, index=False, sheet_name='Inventory Data')
        workbook = writer.book
        worksheet = writer.sheets['Inventory Data']

        # Find the data range (excluding header)
        nrows, ncols = pivot_df.shape
        # Data starts at row 2 (1-indexed for Excel)
        data_range = f'B2:{chr(65+ncols)}{nrows+1}' if ncols <= 26 else f'B2:{chr(64+(ncols//26))+chr(65+(ncols%26))}{nrows+1}'

        # Define formats
        red_format = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        green_format = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
        grey_format = workbook.add_format({'bg_color': '#D9D9D9', 'font_color': '#808080'})

        # Apply conditional formatting for each SOP column
        for col_idx in range(3, ncols):  # skip siteid, year, season
            col_letter = chr(65 + col_idx) if col_idx < 26 else chr(64 + (col_idx // 26)) + chr(65 + (col_idx % 26))
            rng = f'{col_letter}2:{col_letter}{nrows+1}'
            worksheet.conditional_format(rng, {'type': 'text', 'criteria': 'containing', 'value': 'Not Submitted', 'format': red_format})
            worksheet.conditional_format(rng, {'type': 'text', 'criteria': 'containing', 'value': 'Data Available', 'format': green_format})
            worksheet.conditional_format(rng, {'type': 'text', 'criteria': 'containing', 'value': 'Not Assigned', 'format': grey_format})

    output.seek(0)
    return send_file(output,
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                     as_attachment=True,
                     download_name='general_inventory_data.xlsx')

@admin.route('/download-inventory-data-grouped-site', methods=['GET'])
def download_inventory_data_grouped_site():
    eng = create_engine(os.environ.get('DB_CONNECTION_STRING_READONLY'))

    # Query data for the General using Pandas
    general_query = """
        SELECT 
            * 
        FROM
            vw_data_inventory
    """
    with eng.connect() as conn:
        general_df = pd.read_sql(general_query, con=conn)

    sop_name_mapping = {
        "field": "Field Grab",
        "2": "SOP 2: Discrete environmental monitoring - point water quality measurements",
        "3a": "SOP 3: Sediment chemistry",
        "3b": "SOP 3: Sediment toxicity",
        "4": "SOP 4: eDNA - field",
        "5": "SOP 5: Sediment grain size analysis",
        "6a": "SOP 6: Benthic infauna, small",
        "6b": "SOP 6: Benthic infauna, large",
        "7": "SOP 7: Macroalgae",
        "8a": "SOP 8: Fish - BRUVs - Field",
        "8b": "SOP 8: Fish - BRUVs - Lab",
        "9": "SOP 9: Fish seines",
        "10": "SOP 10: Crab traps",
        "11": "SOP 11: Marsh plain vegetation and epifauna surveys",
        "12": "SOP 12: Topographic survey",
        "13": "SOP 13: Sediment accretion rates",
        "15": "SOP 15: Trash monitoring"
    }

    general_df['sop_name'] = general_df['sop'].map(sop_name_mapping)

    # Extract the SOP number for proper sorting
    general_df['sop_number'] = general_df['sop_name'].str.extract(r'SOP (\d+)', expand=False).astype(float)

    # Sort by 'siteid', 'season', 'year', and then by 'sop_number'
    general_df = general_df.sort_values(by=['siteid', 'season', 'year', 'sop_number']).drop(columns=['sop_number'])
    general_df = general_df[['siteid', 'season', 'year', 'sop_name', 'data_exists']]

    # Create an Excel writer object and write the DataFrame to Excel
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        general_df.to_excel(writer, index=False, sheet_name='General Inventory Data')

        # Get the xlsxwriter workbook and worksheet objects
        workbook  = writer.book
        worksheet = writer.sheets['General Inventory Data']

        # Define formats for highlighting
        red_format = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        green_format = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})

        # Apply conditional formatting
        worksheet.conditional_format('E2:E{}'.format(len(general_df) + 1), 
                                     {'type': 'text',
                                      'criteria': 'containing',
                                      'value': 'Not Submitted',
                                      'format': red_format})

        worksheet.conditional_format('E2:E{}'.format(len(general_df) + 1), 
                                     {'type': 'text',
                                      'criteria': 'containing',
                                      'value': 'Data Available',
                                      'format': green_format})

    # Rewind the buffer
    output.seek(0)

    # Send the Excel file to the user
    return send_file(output, 
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 
                     as_attachment=True, 
                     download_name='general_inventory_data.xlsx')

@admin.route('/download-inventory-logger-data', methods=['GET'])
def download_inventory_logger_data():
    eng = create_engine(os.environ.get('DB_CONNECTION_STRING_READONLY'))

    # Query data for the General using Pandas
    general_query = "SELECT * FROM mvw_logger_inventory"
    with eng.connect() as conn:
        general_df = pd.read_sql(general_query, con=conn)

    # Convert DataFrame to CSV
    csv_data = general_df.to_csv(index=False)
    buffer = io.StringIO(csv_data)

    # Send the CSV file to the user
    return send_file(io.BytesIO(buffer.getvalue().encode()), 
                     mimetype='text/csv', 
                     as_attachment=True, 
                     download_name='logger_inventory_data.csv')

@admin.route('/refresh-inventory', methods=['POST'])
def refresh_inventory():
    eng = g.eng
    try:
        with eng.connect() as conn:
            conn.execute(text("SELECT refresh_all_materialized_views();"))
            conn.commit()
        return jsonify({'message': 'Inventory refreshed successfully'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@admin.route('/get-logger-graph-data', methods=['GET'])
def get_logger_graph_data():
    eng = create_engine(os.environ.get('DB_CONNECTION_STRING_READONLY'))
    
    # Extract query parameters
    siteid = request.args.get('siteid')
    year = request.args.get('year')
    month = request.args.get('month')
    parameter = request.args.get('parameter')



    # Ensure all required parameters are present
    if not siteid or not year or not parameter:
        return jsonify({'error': 'Missing required parameters'}), 400

    # Construct the SQL query to fetch data for the specified region, site, year, and parameter
    query = f"""
    SELECT samplecollectiontimestamp, {parameter}
    FROM tbl_wq_logger_raw
    WHERE siteid = '{siteid}' 
    AND EXTRACT(YEAR FROM samplecollectiontimestamp) = {year} 
    AND EXTRACT(MONTH FROM samplecollectiontimestamp) = {month}
    ORDER BY samplecollectiontimestamp ASC
    """
    print(query)
    # Execute the query and load the result into a pandas DataFrame
    with eng.connect() as conn:
        df = pd.read_sql(query, conn)

    # Ensure that there is data for the requested parameter
    if df.empty:
        return jsonify({'error': 'No data found for the specified criteria'}), 404

    # Prepare the data in the format required for the graph (x: samplecollectiontimestamp, y: parameter value)
    graph_data = [{'x': row['samplecollectiontimestamp'], 'y': row[parameter]} for _, row in df.iterrows()]

    print(graph_data)


    # Return the graph data as JSON
    return jsonify(graph_data)




@admin.route('/get-sop1-details', methods=['GET'])
def get_sop1_details():
    """Get SOP 1 (logger) raw_ column details for a specific site, year, and season"""

    siteid = request.args.get('siteid')
    year = request.args.get('year')
    season = request.args.get('season')

    if not all([siteid, year, season]):
        return jsonify({'error': 'Missing required parameters'}), 400

    # Define season month ranges
    def get_month_range(season):
        if season == 'Spring':
            return [3, 4, 5, 6]
        elif season == 'Fall':
            return [7, 8, 9, 10, 11, 12, 1, 2]
        return []

    months = get_month_range(season)
    if not months:
        return jsonify({'error': 'Invalid season'}), 400

    eng = create_engine(os.environ.get('DB_CONNECTION_STRING_READONLY'))

    # Query to get raw_ column values for the specified siteid, year, and season
    query = text("""
        SELECT
            raw_chlorophyll,
            raw_conductivity,
            raw_depth,
            raw_do,
            raw_do_pct,
            raw_h2otemp,
            raw_orp,
            raw_ph,
            raw_pressure,
            raw_qvalue,
            raw_salinity,
            raw_turbidity
        FROM mvw_logger_inventory
        WHERE siteid = :siteid
            AND year = :year
            AND month = ANY(:months)
        LIMIT 1
    """)

    try:
        with eng.connect() as conn:
            result = conn.execute(query, {
                'siteid': siteid,
                'year': int(year),
                'months': months
            }).fetchone()

        if result:
            # Convert row to dictionary
            raw_data = {
                'Chlorophyll': result['raw_chlorophyll'],
                'Conductivity': result['raw_conductivity'],
                'Depth': result['raw_depth'],
                'Dissolved Oxygen': result['raw_do'],
                'DO Percent': result['raw_do_pct'],
                'Temperature': result['raw_h2otemp'],
                'ORP': result['raw_orp'],
                'pH': result['raw_ph'],
                'Pressure': result['raw_pressure'],
                'Q-Value': result['raw_qvalue'],
                'Salinity': result['raw_salinity'],
                'Turbidity': result['raw_turbidity']
            }
            return jsonify({'raw_data': raw_data})
        else:
            return jsonify({'error': 'No data found'}), 404

    except Exception as e:
        print(f"Error fetching SOP 1 details: {e}")
        return jsonify({'error': str(e)}), 500


@admin.route('/get-sop1-table-data', methods=['GET'])
def get_sop1_table_data():
    """Get all SOP 1 raw logger data for the table display"""
    eng = create_engine(os.environ.get('DB_CONNECTION_STRING_READONLY'))

    query = """
        SELECT
            region,
            siteid,
            year,
            month,
            raw_chlorophyll,
            raw_conductivity,
            raw_depth,
            raw_do,
            raw_do_pct,
            raw_h2otemp,
            raw_orp,
            raw_ph,
            raw_pressure,
            raw_qvalue,
            raw_salinity,
            raw_turbidity
        FROM mvw_logger_inventory
        ORDER BY region, siteid, year, month
    """

    try:
        df = pd.read_sql(query, eng)

        # Convert to list of dictionaries
        data = df.to_dict('records')

        # Get unique years for filter
        years = sorted(df['year'].unique().tolist())

        return jsonify({
            'data': data,
            'years': years
        })
    except Exception as e:
        print(f"Error fetching SOP 1 table data: {e}")
        return jsonify({'error': str(e)}), 500


@admin.route('/get-sample-data', methods=['GET'])
def get_sample_data():

    # SOP to table mapping
    sop_choices = {
        "sopfield": "tbl_grabevent",
        "sop2": "tbl_waterquality_metadata",
        "sop3a": "tbl_sedchem_labbatch_data",
        "sop3b": "tbl_toxicitysummary",
        "sop4": "tbl_edna_metadata",
        "sop5": "tbl_sedgrainsize_labbatch_data",
        "sop6a": "tbl_benthicinfauna_labbatch",
        "sop6b": "tbl_benthiclarge_metadata",
        "sop7": "tbl_macroalgae_site_meta",
        "sop8a": "tbl_bruv_metadata",
        "sop8b": "tbl_bruv_data",
        "sop9": "tbl_fish_sample_metadata",
        "sop10": "tbl_crabtrap_metadata",
        "sop11": "tbl_vegetation_sample_metadata",
        "sop13": "tbl_feldspar_metadata",
        "sop15": "tbl_trashsamplearea"
    }

    def get_date_range(year, season):
        """Returns the start and end date based on the season and year."""
        if season == 'Spring':
            start_date = f"{year}-03-01"
            end_date = f"{year}-06-30"
        elif season == 'Fall':
            start_date = f"{year}-07-01"
            end_date = f"{int(year) + 1}-02-28"  # Next year's February
        else:
            return None, None
        return start_date, end_date


    # Fetch request parameters
    sop_name = request.args.get('sop')
    site_id = request.args.get('siteid')
    year = request.args.get('year')
    season = request.args.get('season')

    # Get table name based on SOP
    table_name = sop_choices.get(sop_name)
    if not table_name:
        return jsonify({'error': 'Invalid SOP'}), 400

    # Get the date range for the query based on the season
    start_date, end_date = get_date_range(year, season)

    if not start_date or not end_date:
        return jsonify({'error': 'Invalid season or year'}), 400

    # Establish database connection
    eng = create_engine(os.environ.get('DB_CONNECTION_STRING_READONLY'))

    # Query the database for the sample collection date and created date

    query = f"""
    SELECT samplecollectiondate, created_date
    FROM {table_name}
    WHERE siteid = :site_id
    AND samplecollectiondate >= :start_date
    AND samplecollectiondate <= :end_date
    ORDER BY samplecollectiondate;
    """

    try:
        # Execute the query and fetch all results
        with eng.connect() as connection:
            result = connection.execute(text(query), {
                'site_id': site_id,
                'start_date': start_date,
                'end_date': end_date
            }).fetchall()

        # If data is found, process and join the dates
        if result:
            # Extract and sort the samplecollectiondate and created_date
            samplecollectiondate = sorted(set(
                row['samplecollectiondate'].strftime('%Y-%m-%d') for row in result if row['samplecollectiondate']
            ))

            created_dates = sorted(set(
                row['created_date'].strftime('%Y-%m-%d') for row in result if row['created_date']
            ))
            
            # Join the sorted dates into a comma-separated string
            return jsonify({
                'samplecollectiondate': ', '.join(samplecollectiondate) if samplecollectiondate else 'N/A',
                'created_date': ', '.join(created_dates) if created_dates else 'N/A'
            })
        else:
            return jsonify({'samplecollectiondate': 'N/A', 'created_date': 'N/A'})


    except Exception as e:
        print(e)
        return jsonify({'error': str(e)}), 500


@admin.route('/view-all-polygons', methods=['GET'])
def view_all_polygons():
    """Route to display all EMPA station polygons with dropdown filter by estuary"""
    return render_template('view_all_polygons.html')


@admin.route('/get-all-polygons-data', methods=['GET'])
def get_all_polygons_data():
    """API endpoint to fetch polygon data from both spatial_empa_all_sites and spatial_empa_all_stations tables"""
    try:
        eng = create_engine(os.environ.get('DB_CONNECTION_STRING_READONLY'))

        # Query to get estuary polygons (red)
        estuary_query = """
            SELECT
                estuaryname,
                ST_AsGeoJSON(geometry) as geometry
            FROM
                spatial_empa_all_sites
            ORDER BY
                estuaryname
        """

        # Query to get station polygons (blue)
        station_query = """
            SELECT
                estuaryname,
                siteid,
                stationno,
                ST_AsGeoJSON(geometry) as geometry
            FROM
                spatial_empa_all_stations
            ORDER BY
                estuaryname,
                stationno
        """

        with eng.connect() as connection:
            estuary_result = connection.execute(text(estuary_query)).fetchall()
            station_result = connection.execute(text(station_query)).fetchall()

        # Structure the data
        data = {
            'estuaries': [],
            'stations': []
        }

        # Add estuary polygons
        for row in estuary_result:
            data['estuaries'].append({
                'estuaryname': row['estuaryname'],
                'geometry': row['geometry']
            })

        # Add station polygons
        for row in station_result:
            data['stations'].append({
                'estuaryname': row['estuaryname'],
                'siteid': row['siteid'],
                'stationno': row['stationno'],
                'geometry': row['geometry']
            })

        return jsonify(data)
    
    except Exception as e:
        print(f"Error fetching polygon data: {e}")
        return jsonify({'error': str(e)}), 500


@admin.route('/save-station-qa', methods=['POST'])
def save_station_qa():
    """API endpoint to save QA action (confirm or edit) for a station"""
    try:
        data = request.get_json()
        
        sop = data.get('sop')
        region = data.get('region')
        siteid = data.get('siteid')
        objectids = data.get('objectids')
        action = data.get('action')
        comment = data.get('comment', '')
        last_edited_user = data.get('last_edited_user', '')
        
        if not all([sop, siteid, objectids, action, last_edited_user]):
            return jsonify({'error': 'Missing required fields'}), 400
        
        eng = create_engine(os.environ.get('DB_CONNECTION_STRING_STATIONSQA'))
        
        insert_query = text("""
            INSERT INTO spatial_stations_qa (sop, region, siteid, objectids, action, comment, last_edited_user, last_edited_date)
            VALUES (:sop, :region, :siteid, :objectids, :action, :comment, :last_edited_user, NOW())
        """)
        
        with eng.begin() as connection:
            connection.execute(insert_query, {
                'sop': sop,
                'region': region,
                'siteid': siteid,
                'objectids': objectids,
                'action': action,
                'comment': comment,
                'last_edited_user': last_edited_user
            })
        
        return jsonify({'message': 'QA action saved successfully'}), 200
    
    except Exception as e:
        print(f"Error saving station QA: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@admin.route('/get-all-regions', methods=['GET'])
def get_all_regions():
    """API endpoint to fetch all unique regions from search table"""
    try:
        eng = create_engine(os.environ.get('DB_CONNECTION_STRING_READONLY'))
        
        query = text("""
            SELECT DISTINCT region
            FROM search
            WHERE region IS NOT NULL
            ORDER BY region
        """)
        
        with eng.connect() as conn:
            result = conn.execute(query)
            regions = [row[0] for row in result]
        
        return jsonify({'regions': regions})
    except Exception as e:
        print(f"Error fetching regions: {str(e)}")
        return jsonify({'error': str(e)}), 500

@admin.route('/get-sop-station-data', methods=['GET'])
def get_sop_station_data():
    """API endpoint to fetch station data from SOP metadata table and verify polygon matches"""
    try:
        table_name = request.args.get('table')
        if not table_name:
            return jsonify({'error': 'Table name required'}), 400
        
        eng = create_engine(os.environ.get('DB_CONNECTION_STRING_READONLY'))
        
        # Handle different column names for different tables
        lat_col = 'latitude'
        long_col = 'longitude'
        
        if table_name == 'tbl_fish_sample_metadata':
            lat_col = 'netbeginlatitude'
            long_col = 'netbeginlongitude'
        elif table_name == 'tbl_macroalgae_sample_metadata':
            lat_col = 'transectbeginlatitude'
            long_col = 'transectbeginlongitude'
        
        # Query to get metadata points and check if they fall within station polygons
        query = f"""
            WITH meta_points AS (
                SELECT DISTINCT
                    STRING_AGG(DISTINCT t.objectid::text, ', ' ORDER BY t.objectid::text) as objectid,
                    t.siteid,
                    t.stationno,
                    t.{lat_col} as latitude,
                    t.{long_col} as longitude,
                    ST_SetSRID(ST_MakePoint(t.{long_col}, t.{lat_col}), 4326) as geom,
                    STRING_AGG(DISTINCT TO_CHAR(t.samplecollectiondate, 'YYYY-MM-DD'), ', ' ORDER BY TO_CHAR(t.samplecollectiondate, 'YYYY-MM-DD')) as samplecollectiondate,
                    s.region
                FROM {table_name} t
                LEFT JOIN search s ON t.siteid = s.siteid
                WHERE t.{lat_col} IS NOT NULL AND t.{long_col} IS NOT NULL
                    AND t.{lat_col} <> -88 AND t.{long_col} <> -88
                GROUP BY t.siteid, t.stationno, t.{lat_col}, t.{long_col}, s.region
            ),
            station_polygons AS (
                SELECT 
                    siteid,
                    stationno,
                    geometry,
                    estuaryname as sitename
                FROM spatial_empa_all_stations
            )
            SELECT 
                mp.objectid,
                mp.siteid as siteid_meta,
                mp.stationno as stationno_meta,
                mp.latitude,
                mp.longitude,
                mp.samplecollectiondate,
                mp.region,
                sp.stationno as stationno_polygon,
                sp.sitename,
                CASE 
                    WHEN sp.stationno IS NOT NULL AND mp.stationno = sp.stationno THEN 'Match'
                    WHEN sp.stationno IS NOT NULL AND mp.stationno != sp.stationno THEN 'No Match'
                    ELSE 'Not in Polygon'
                END as match_status,
                ST_AsGeoJSON(sp.geometry) as polygon_geometry,
                qa.action as qa_action,
                qa.last_edited_date as qa_last_edited_date
            FROM meta_points mp
            LEFT JOIN station_polygons sp ON ST_Within(mp.geom, sp.geometry)
            LEFT JOIN LATERAL (
                SELECT action, last_edited_date, sop
                FROM spatial_stations_qa
                WHERE spatial_stations_qa.siteid = mp.siteid
                    AND EXISTS (
                        SELECT 1
                        FROM unnest(string_to_array(spatial_stations_qa.objectids, ', ')) AS qa_oid
                        WHERE qa_oid = ANY(string_to_array(mp.objectid, ', '))
                    )
                ORDER BY last_edited_date DESC
                LIMIT 1
            ) qa ON true
            ORDER BY mp.region, mp.siteid, mp.stationno, mp.objectid
        """
        print(query)
       
        with eng.connect() as connection:
            result = connection.execute(text(query)).fetchall()
        
        # Structure the data
        data = {
            'points': [],
            'bad_points': []
        }
        
        for row in result:
            point_data = {
                'objectid': row['objectid'],
                'siteid_meta': row['siteid_meta'],
                'stationno_meta': row['stationno_meta'],
                'latitude': float(row['latitude']) if row['latitude'] else None,
                'longitude': float(row['longitude']) if row['longitude'] else None,
                'samplecollectiondates': row['samplecollectiondate'] if row['samplecollectiondate'] else 'N/A',
                'region': row['region'],
                'stationno_polygon': row['stationno_polygon'],
                'sitename': row['sitename'],
                'match_status': row['match_status'],
                'polygon_geometry': row['polygon_geometry'],
                'qa_action': row['qa_action']
            }
            
            data['points'].append(point_data)
            
            if row['match_status'] in ['No Match', 'Not in Polygon']:
                data['bad_points'].append(point_data)
        
        return jsonify(data)

    except Exception as e:
        print(f"Error fetching SOP station data: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@admin.route('/download-polygons-shapefile', methods=['GET'])
def download_polygons_shapefile():
    """Download estuaries and stations polygons as a shapefile zip"""
    try:
        eng = create_engine(os.environ.get('DB_CONNECTION_STRING_READONLY'))

        # Get selected estuaries from query parameter (comma-separated)
        selected_estuaries = request.args.get('estuaries', '')
        estuary_list = [e.strip() for e in selected_estuaries.split(',') if e.strip()] if selected_estuaries else []

        # Build WHERE clause if estuaries are selected
        if estuary_list:
            placeholders = ', '.join([f"'{e}'" for e in estuary_list])
            estuary_where = f"WHERE estuaryname IN ({placeholders})"
            station_where = f"WHERE estuaryname IN ({placeholders})"
        else:
            estuary_where = ""
            station_where = ""

        # Query to get estuary polygons with geometry
        estuary_query = f"""
            SELECT
                estuaryname,
                geometry
            FROM
                spatial_empa_all_sites
            {estuary_where}
            ORDER BY
                estuaryname
        """
        print(station_where)
        # Query to get station polygons with geometry
        station_query = f"""
            SELECT
                estuaryname,
                siteid,
                stationno,
                geometry
            FROM
                spatial_empa_all_stations
            {station_where}
            ORDER BY
                estuaryname,
                stationno
        """

        # Read data as GeoDataFrames directly from PostGIS
        gdf_estuaries = gpd.read_postgis(estuary_query, eng, geom_col='geometry')
        gdf_stations = gpd.read_postgis(station_query, eng, geom_col='geometry')

        # Create a temporary directory to store shapefiles
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create subdirectories for each layer
            estuaries_dir = os.path.join(tmpdir, 'estuaries')
            stations_dir = os.path.join(tmpdir, 'stations')
            os.makedirs(estuaries_dir)
            os.makedirs(stations_dir)

            # Save estuaries shapefile
            estuaries_shp_path = os.path.join(estuaries_dir, 'estuaries.shp')
            gdf_estuaries.to_file(estuaries_shp_path)

            # Save stations shapefile
            stations_shp_path = os.path.join(stations_dir, 'stations.shp')
            gdf_stations.to_file(stations_shp_path)

            # Create a zip file containing both shapefiles
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # Add estuary shapefile components
                for filename in os.listdir(estuaries_dir):
                    file_path = os.path.join(estuaries_dir, filename)
                    zipf.write(file_path, os.path.join('estuaries', filename))

                # Add station shapefile components
                for filename in os.listdir(stations_dir):
                    file_path = os.path.join(stations_dir, filename)
                    zipf.write(file_path, os.path.join('stations', filename))

            zip_buffer.seek(0)

            return send_file(
                zip_buffer,
                mimetype='application/zip',
                as_attachment=True,
                download_name='empa_polygons.zip'
            )

    except Exception as e:
        print(f"Error downloading shapefile: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@admin.route('/new-project-metadata-form', methods=['GET', 'POST'])
def new_project_metadata_form():
    """New Project Metadata Form"""
    authorized = session.get("AUTHORIZED_FOR_ADMIN_FUNCTIONS")
    
    if request.method == 'GET':
        return render_template('new_project_metadata_form.html', authorized=authorized)
    
    elif request.method == 'POST':
        try:
            form_data = request.form

            # Validate email
            email = form_data.get('email', '').strip()
            if not email or not re.match(r'^[^\s@]+@[^\s@]+\.[^\s@]+$', email):
                return jsonify({'status': 'error', 'message': 'A valid email address is required'}), 400

            # Validate abbreviation
            abbreviation = form_data.get('abbreviation', '').strip()
            if not abbreviation:
                return jsonify({'status': 'error', 'message': 'Project abbreviation is required'}), 400

            # Create upload directory: export/{email}/
            upload_dir = os.path.join(os.getcwd(), 'export', email)
            os.makedirs(upload_dir, exist_ok=True)

            # --- Collect agencies (dynamic fields) ---
            agencies = []
            i = 1
            while True:
                agency_name = form_data.get(f'agency_{i}_name', '').strip()
                agency_role = form_data.get(f'agency_{i}_role', '').strip()
                if not agency_name and not agency_role:
                    break
                if agency_name:
                    agencies.append({'name': agency_name, 'role': agency_role})
                i += 1

            # --- Collect estuaries (multi-checkbox) ---
            estuaries_list = request.form.getlist('estuary')
            estuaries_str = ','.join(estuaries_list)

            # --- Collect polygons (dynamic fields + shapefile uploads) ---
            polygons = []
            p = 1
            while True:
                poly_estuary = form_data.get(f'polygon_{p}_estuary', '').strip()
                poly_siteid = form_data.get(f'polygon_{p}_siteid', '').strip()
                if not poly_estuary and not poly_siteid:
                    break

                shapefile_path = None
                shapefile_key = f'polygon_{p}_shapefile'
                if shapefile_key in request.files:
                    file = request.files[shapefile_key]
                    if file and file.filename:
                        if not file.filename.lower().endswith('.zip'):
                            return jsonify({'status': 'error', 'message': f'Invalid file type for {file.filename}. Only .zip files are allowed for shapefiles.'}), 400
                        timestamp = int(time.time())
                        filename = f"polygon_{p}_{timestamp}.zip"
                        filepath = os.path.join(upload_dir, filename)
                        file.save(filepath)
                        shapefile_path = os.path.join('export', email, filename)

                polygons.append({
                    'estuary': poly_estuary,
                    'siteid': poly_siteid,
                    'shapefile_path': shapefile_path
                })
                p += 1

            # --- Collect SOPs and their detail fields + file uploads ---
            selected_sops = request.form.getlist('sop')
            sop_records = []
            for sop_id in selected_sops:
                # Handle SOP PDF upload
                sop_file_path = None
                file_key = f'{sop_id}_file'
                if file_key in request.files:
                    file = request.files[file_key]
                    if file and file.filename:
                        if not file.filename.lower().endswith('.pdf'):
                            return jsonify({'status': 'error', 'message': f'Invalid file type for {file.filename}. Only PDF files are allowed.'}), 400
                        timestamp = int(time.time())
                        filename = f"{sop_id}_{timestamp}.pdf"
                        filepath = os.path.join(upload_dir, filename)
                        file.save(filepath)
                        sop_file_path = os.path.join('export', email, filename)

                sop_records.append({
                    'sop_id': sop_id,
                    'sop_file_path': sop_file_path,
                    'data_purpose': form_data.get(f'{sop_id}_data_purpose', '').strip() or None,
                    'specific_guidelines': form_data.get(f'{sop_id}_specific_guidelines', '').strip() or None,
                    'dataset_start_date': form_data.get(f'{sop_id}_dataset_start_date', '').strip() or None,
                    'dataset_end_date': form_data.get(f'{sop_id}_dataset_end_date', '').strip() or None,
                    'west_bounding': form_data.get(f'{sop_id}_west_bounding', '').strip() or None,
                    'east_bounding': form_data.get(f'{sop_id}_east_bounding', '').strip() or None,
                    'north_bounding': form_data.get(f'{sop_id}_north_bounding', '').strip() or None,
                    'south_bounding': form_data.get(f'{sop_id}_south_bounding', '').strip() or None,
                    'coordinate_system': form_data.get(f'{sop_id}_coordinate_system', '').strip() or None,
                    'location_accuracy': form_data.get(f'{sop_id}_location_accuracy', '').strip() or None,
                    'legal_restrictions': form_data.get(f'{sop_id}_legal_restrictions', '').strip() or None,
                    'data_gaps': form_data.get(f'{sop_id}_data_gaps', '').strip() or None,
                })

            # --- Database persistence ---
            eng = g.eng

            with eng.begin() as connection:
                # Delete existing submission for this email (upsert behavior)
                connection.execute(
                    text("DELETE FROM project_metadata WHERE email = :email"),
                    {'email': email}
                )

                # Insert into project_metadata
                result = connection.execute(
                    text("""
                        INSERT INTO project_metadata 
                            (email, contact_name, institution, phone, project_name, abbreviation, 
                             main_goals, project_start, project_end, estuaries, agencies, polygons)
                        VALUES 
                            (:email, :contact_name, :institution, :phone, :project_name, :abbreviation,
                             :main_goals, :project_start, :project_end, :estuaries, :agencies, :polygons)
                        RETURNING id
                    """),
                    {
                        'email': email,
                        'contact_name': form_data.get('contactName', '').strip(),
                        'institution': form_data.get('institution', '').strip(),
                        'phone': form_data.get('phone', '').strip(),
                        'project_name': form_data.get('projectName', '').strip(),
                        'abbreviation': abbreviation,
                        'main_goals': form_data.get('mainGoals', '').strip(),
                        'project_start': form_data.get('projectStart', '').strip() or None,
                        'project_end': form_data.get('projectEnd', '').strip() or None,
                        'estuaries': estuaries_str,
                        'agencies': json.dumps(agencies),
                        'polygons': json.dumps(polygons),
                    }
                )
                project_id = result.fetchone()[0]

                # Insert SOP records
                for sop in sop_records:
                    connection.execute(
                        text("""
                            INSERT INTO project_sops
                                (project_id, sop_id, sop_file_path, data_purpose, specific_guidelines,
                                 dataset_start_date, dataset_end_date, west_bounding, east_bounding,
                                 north_bounding, south_bounding, coordinate_system, location_accuracy,
                                 legal_restrictions, data_gaps)
                            VALUES
                                (:project_id, :sop_id, :sop_file_path, :data_purpose, :specific_guidelines,
                                 :dataset_start_date, :dataset_end_date, :west_bounding, :east_bounding,
                                 :north_bounding, :south_bounding, :coordinate_system, :location_accuracy,
                                 :legal_restrictions, :data_gaps)
                        """),
                        {
                            'project_id': project_id,
                            'sop_id': sop['sop_id'],
                            'sop_file_path': sop['sop_file_path'],
                            'data_purpose': sop['data_purpose'],
                            'specific_guidelines': sop['specific_guidelines'],
                            'dataset_start_date': sop['dataset_start_date'],
                            'dataset_end_date': sop['dataset_end_date'],
                            'west_bounding': sop['west_bounding'],
                            'east_bounding': sop['east_bounding'],
                            'north_bounding': sop['north_bounding'],
                            'south_bounding': sop['south_bounding'],
                            'coordinate_system': sop['coordinate_system'],
                            'location_accuracy': sop['location_accuracy'],
                            'legal_restrictions': sop['legal_restrictions'],
                            'data_gaps': sop['data_gaps'],
                        }
                    )

            return jsonify({
                'status': 'success',
                'message': 'Project metadata submitted successfully'
            })

        except Exception as e:
            print(f"Error submitting project metadata form: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({
                'status': 'error',
                'message': str(e)
            }), 500


# How long (seconds) an emailed sign-in code stays valid
EDIT_CODE_TTL = 10 * 60


@admin.route('/api/edit-request-code', methods=['POST'])
def edit_request_code():
    """
    Step 1 of edit sign-in: a user enters the email they originally submitted with.
    If a project_metadata record exists for that email, we generate a 6-digit code,
    stash it in the session, and email it to them.
    """
    try:
        data = request.get_json() or {}
        email = (data.get('email') or '').strip()

        if not email or not re.match(r'^[^\s@]+@[^\s@]+\.[^\s@]+$', email):
            return jsonify({'status': 'error', 'message': 'A valid email address is required'}), 400

        # Confirm a submission exists for this email
        eng = g.eng
        with eng.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM project_metadata WHERE email = :email LIMIT 1"),
                {'email': email}
            ).fetchone()

        if not exists:
            return jsonify({'status': 'error', 'message': 'No project submission was found for this email address.'}), 404

        # Generate a 6-digit code and stash it in the session
        code = f"{random.randint(0, 999999):06d}"
        session['edit_code'] = code
        session['edit_code_email'] = email
        session['edit_code_expiry'] = time.time() + EDIT_CODE_TTL
        # A fresh code request invalidates any prior verified edit session
        session.pop('edit_verified_email', None)

        msgbody = (
            f"Hello,\n\n"
            f"Here is your verification code to edit your {current_app.project_name} project metadata submission:\n\n"
            f"    {code}\n\n"
            f"This code will expire in {EDIT_CODE_TTL // 60} minutes. "
            f"If you did not request this, you can ignore this email.\n"
        )

        send_mail(
            current_app.mail_from,
            [email],
            f"{current_app.project_name} - Project Metadata Edit Verification Code",
            msgbody,
            server=current_app.config['MAIL_SERVER']
        )

        return jsonify({'status': 'success', 'message': f'A verification code was sent to {email}.'})

    except Exception as e:
        print(f"Error requesting edit code: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': 'Unable to send verification code. Please try again.'}), 500


@admin.route('/api/edit-verify-code', methods=['POST'])
def edit_verify_code():
    """
    Step 2 of edit sign-in: the user submits the 6-digit code we emailed them.
    On success we mark the session as verified for that email and return the
    existing submission so the form can be pre-filled for editing.
    """
    try:
        data = request.get_json() or {}
        email = (data.get('email') or '').strip()
        code = (data.get('code') or '').strip()

        stored_code = session.get('edit_code')
        stored_email = session.get('edit_code_email')
        expiry = session.get('edit_code_expiry', 0)

        if not stored_code or not stored_email:
            return jsonify({'status': 'error', 'message': 'No code was requested. Please request a new code.'}), 400

        if time.time() > expiry:
            session.pop('edit_code', None)
            session.pop('edit_code_email', None)
            session.pop('edit_code_expiry', None)
            return jsonify({'status': 'error', 'message': 'This code has expired. Please request a new one.'}), 400

        if email != stored_email or code != stored_code:
            return jsonify({'status': 'error', 'message': 'Invalid verification code.'}), 401

        # Verified — mark the session and clear the one-time code
        session['edit_verified_email'] = email
        session.pop('edit_code', None)
        session.pop('edit_code_email', None)
        session.pop('edit_code_expiry', None)

        # Fetch the existing record so the front end can pre-fill the form
        eng = g.eng
        with eng.connect() as conn:
            row = conn.execute(
                text("SELECT * FROM project_metadata WHERE email = :email"),
                {'email': email}
            ).fetchone()

            if not row:
                return jsonify({'status': 'error', 'message': 'No project submission was found for this email address.'}), 404

            project = dict(row._mapping)
            project['project_start'] = str(project['project_start']) if project.get('project_start') else ''
            project['project_end'] = str(project['project_end']) if project.get('project_end') else ''
            project['agencies_list'] = json.loads(project['agencies']) if project.get('agencies') else []
            project['polygons_list'] = json.loads(project['polygons']) if project.get('polygons') else []
            project['estuaries_list'] = project['estuaries'].split(',') if project.get('estuaries') else []

            sops_result = conn.execute(
                text("SELECT * FROM project_sops WHERE project_id = :pid ORDER BY sop_id"),
                {'pid': project['id']}
            )
            sops = []
            for r in sops_result:
                s = dict(r._mapping)
                s['dataset_start_date'] = str(s['dataset_start_date']) if s.get('dataset_start_date') else ''
                s['dataset_end_date'] = str(s['dataset_end_date']) if s.get('dataset_end_date') else ''
                s['west_bounding'] = float(s['west_bounding']) if s.get('west_bounding') is not None else None
                s['east_bounding'] = float(s['east_bounding']) if s.get('east_bounding') is not None else None
                s['north_bounding'] = float(s['north_bounding']) if s.get('north_bounding') is not None else None
                s['south_bounding'] = float(s['south_bounding']) if s.get('south_bounding') is not None else None
                sops.append(s)
            project['sops'] = sops

        # Drop raw JSON strings we don't need on the client
        project.pop('agencies', None)
        project.pop('polygons', None)
        for key in ('created_date', 'updated_date'):
            if project.get(key) is not None:
                project[key] = str(project[key])

        return jsonify({'status': 'success', 'data': project})

    except Exception as e:
        print(f"Error verifying edit code: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': 'Unable to verify code. Please try again.'}), 500


@admin.route('/api/project-metadata-admin', methods=['POST'])
def project_metadata_admin_api():
    """API endpoint to fetch all submitted project metadata after password verification"""
    try:
        data = request.get_json()
        password = data.get('password', '')

        if password != os.environ.get('ADMIN_FUNCTION_PASSWORD'):
            return jsonify({'status': 'error', 'message': 'Invalid password'}), 401

        eng = g.eng
        with eng.connect() as conn:
            # Get all projects with SOP details
            projects_result = conn.execute(text("""
                SELECT 
                    pm.*,
                    COUNT(ps.id) as sop_count
                FROM project_metadata pm
                LEFT JOIN project_sops ps ON pm.id = ps.project_id
                GROUP BY pm.id
                ORDER BY pm.created_date DESC
            """))
            projects = []
            for r in projects_result:
                p = dict(r._mapping)
                p['created_date'] = p['created_date'].strftime('%Y-%m-%d %H:%M') if p.get('created_date') else 'N/A'
                p['updated_date'] = p['updated_date'].strftime('%Y-%m-%d %H:%M') if p.get('updated_date') else 'N/A'
                p['project_start'] = str(p['project_start']) if p.get('project_start') else 'N/A'
                p['project_end'] = str(p['project_end']) if p.get('project_end') else 'N/A'
                p['agencies_list'] = json.loads(p['agencies']) if p.get('agencies') else []
                p['polygons_list'] = json.loads(p['polygons']) if p.get('polygons') else []
                p['estuaries_list'] = p['estuaries'].split(',') if p.get('estuaries') else []
                projects.append(p)

            # Get all SOPs grouped by project
            sops_result = conn.execute(text("""
                SELECT * FROM project_sops ORDER BY project_id, sop_id
            """))
            sops_by_project = {}
            for r in sops_result:
                s = dict(r._mapping)
                s['dataset_start_date'] = str(s['dataset_start_date']) if s.get('dataset_start_date') else None
                s['dataset_end_date'] = str(s['dataset_end_date']) if s.get('dataset_end_date') else None
                s['west_bounding'] = float(s['west_bounding']) if s.get('west_bounding') is not None else None
                s['east_bounding'] = float(s['east_bounding']) if s.get('east_bounding') is not None else None
                s['north_bounding'] = float(s['north_bounding']) if s.get('north_bounding') is not None else None
                s['south_bounding'] = float(s['south_bounding']) if s.get('south_bounding') is not None else None
                pid = s['project_id']
                if pid not in sops_by_project:
                    sops_by_project[pid] = []
                sops_by_project[pid].append(s)

            # Attach SOPs to projects
            for p in projects:
                p['sops'] = sops_by_project.get(p['id'], [])

        return jsonify({'status': 'success', 'data': projects})
    except Exception as e:
        print(f"Error in project metadata admin API: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500


@admin.route('/project-metadata-list', methods=['GET'])
def project_metadata_list():
    """Admin view to list all submitted project metadata"""
    try:
        eng = g.eng
        with eng.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    pm.id,
                    pm.email,
                    pm.project_name,
                    pm.abbreviation,
                    pm.contact_name,
                    pm.institution,
                    pm.created_date,
                    COUNT(ps.id) as sop_count
                FROM project_metadata pm
                LEFT JOIN project_sops ps ON pm.id = ps.project_id
                GROUP BY pm.id, pm.email, pm.project_name, pm.abbreviation, 
                         pm.contact_name, pm.institution, pm.created_date
                ORDER BY pm.created_date DESC
            """))
            projects = [dict(r._mapping) for r in result]
        return render_template('project_metadata_list.html', projects=projects)
    except Exception as e:
        print(f"Error loading project metadata list: {e}")
        import traceback
        traceback.print_exc()
        return f"Error: {str(e)}", 500


@admin.route('/project-metadata/<int:project_id>', methods=['GET'])
def project_metadata_detail(project_id):
    """Admin view to see detail of a submitted project"""
    try:
        eng = g.eng
        with eng.connect() as conn:
            # Get project metadata
            result = conn.execute(
                text("SELECT * FROM project_metadata WHERE id = :pid"),
                {'pid': project_id}
            )
            row = result.fetchone()
            if not row:
                return "Project not found", 404
            project = dict(row._mapping)

            # Parse JSON fields
            project['agencies_list'] = json.loads(project['agencies']) if project.get('agencies') else []
            project['polygons_list'] = json.loads(project['polygons']) if project.get('polygons') else []
            project['estuaries_list'] = project['estuaries'].split(',') if project.get('estuaries') else []

            # Get SOPs
            sops_result = conn.execute(
                text("SELECT * FROM project_sops WHERE project_id = :pid ORDER BY sop_id"),
                {'pid': project_id}
            )
            sops = [dict(r._mapping) for r in sops_result]

        return render_template('project_metadata_detail.html', project=project, sops=sops)
    except Exception as e:
        print(f"Error loading project metadata detail: {e}")
        import traceback
        traceback.print_exc()
        return f"Error: {str(e)}", 500


@admin.route('/download-project-file', methods=['GET'])
def download_project_file():
    """Download an uploaded project file (SOP PDF or polygon shapefile)"""
    try:
        file_path = request.args.get('path', '')
        if not file_path:
            return "No file path provided", 400

        # Security: ensure the path is within export/ directory
        full_path = os.path.join(os.getcwd(), file_path)
        abs_export = os.path.abspath(os.path.join(os.getcwd(), 'export'))
        abs_file = os.path.abspath(full_path)

        if not abs_file.startswith(abs_export):
            return "Access denied", 403

        if not os.path.exists(full_path):
            return "File not found", 404

        return send_file(full_path, as_attachment=True)
    except Exception as e:
        print(f"Error downloading project file: {e}")
        return f"Error: {str(e)}", 500


@admin.route('/api/get-estuaries', methods=['GET'])
def get_estuaries():
    """API endpoint to fetch estuary data from lu_siteid table with geometry"""
    try:
        eng = create_engine(os.environ.get('DB_CONNECTION_STRING_READONLY'))
        
        with eng.connect() as conn:
            # Get unique siteids with their estuary names and geometry from spatial_empa_all_sites
            query = text("""
                SELECT DISTINCT 
                    l.siteid, 
                    l.estuary,
                    ST_AsGeoJSON(s.geometry) as geometry
                FROM lu_siteid l
                LEFT JOIN spatial_empa_all_sites s ON l.siteid = s.siteid
                ORDER BY l.estuary, l.siteid
            """)
            result = conn.execute(query)
            estuaries = [
                {
                    'siteid': row.siteid, 
                    'estuary': row.estuary,
                    'geometry': row.geometry
                } 
                for row in result
            ]
        
        return jsonify({
            'status': 'success',
            'data': estuaries
        })
    except Exception as e:
        print(f"Error fetching estuaries: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@admin.route('/get-estuaries-by-region', methods=['GET'])
def get_estuaries_by_region():
    """API endpoint to fetch estuaries grouped by region"""
    try:
        eng = create_engine(os.environ.get('DB_CONNECTION_STRING_READONLY'))

        query = text("""
            SELECT DISTINCT
                s.region,
                sa.estuaryname
            FROM spatial_empa_all_stations sa
            JOIN search s ON sa.siteid = s.siteid
            WHERE s.region IS NOT NULL AND sa.estuaryname IS NOT NULL
            ORDER BY s.region, sa.estuaryname
        """)

        with eng.connect() as conn:
            result = conn.execute(query)
            rows = [dict(r._mapping) for r in result]

        # Group by region
        region_estuaries = {}
        for row in rows:
            region = row['region']
            estuary = row['estuaryname']
            if region not in region_estuaries:
                region_estuaries[region] = []
            if estuary not in region_estuaries[region]:
                region_estuaries[region].append(estuary)

        return jsonify({'regions': region_estuaries})
    except Exception as e:
        print(f"Error fetching estuaries by region: {e}")
        return jsonify({'error': str(e)}), 500
