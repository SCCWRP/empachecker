import os, time, re
import pandas as pd
from bs4 import BeautifulSoup
from flask import Blueprint, g, current_app, render_template, redirect, url_for, session, request, jsonify, render_template_string, send_file
from io import StringIO
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, text
import io

from .utils.db import metadata_summary
from .utils.route_auth import requires_auth

admin = Blueprint('admin', __name__)

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
    session_results = g.eng.execute(sql_session)
    session_json = [dict(r) for r in session_results]
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
                df = pd.read_sql(f"{basequery} WHERE table_name = '{tbl}';", eng)

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
@requires_auth
def report():
    return render_template("inventory-main.html")

@admin.route('/report-download', methods=['GET', 'POST'])
def report_download():
    # Return as a downloadable file
    return send_file(os.path.join(os.getcwd(), 'export','inventory-report.csv'), as_attachment=True, download_name="report.csv", mimetype="text/csv")


@admin.route('/get-inventory-data', methods=['GET'])
def get_inventory_data():
    eng = create_engine(os.environ.get('DB_CONNECTION_STRING_READONLY'))

    # Query data for the Logger using Pandas
    logger_query = "SELECT * FROM mvw_qa_raw_logger_combined_final"
    logger_df = pd.read_sql(logger_query, con=eng)
    logger_df['year'] = logger_df['year'].astype(int).astype(str)

    # Query data for the General using Pandas
    general_query = \
        """
            SELECT DISTINCT
                sop,
                region,
                siteid,
                year,
                season,
                data_exists
            FROM
                vw_qa_allsop_combined_final 
            ORDER BY
                sop,
                region,
                siteid,
                year
        """
    general_df = pd.read_sql(general_query, con=eng)
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
            
    return jsonify(inventory_data)

@admin.route('/download-inventory-data', methods=['GET'])
def download_inventory_data():
    eng = create_engine(os.environ.get('DB_CONNECTION_STRING_READONLY'))

    # Query data for the General using Pandas
    general_query = """
        SELECT DISTINCT
            sop,
            region,
            siteid,
            year,
            season,
            months_with_data,
            data_exists
        FROM
            vw_qa_allsop_combined_final 
        ORDER BY
            sop,
            region,
            siteid,
            year
    """
    general_df = pd.read_sql(general_query, con=eng)

    # Convert DataFrame to CSV
    csv_data = general_df.to_csv(index=False)
    buffer = io.StringIO(csv_data)

    # Send the CSV file to the user
    return send_file(io.BytesIO(buffer.getvalue().encode()), 
                     mimetype='text/csv', 
                     as_attachment=True, 
                     download_name='general_inventory_data.csv')

@admin.route('/download-inventory-data-grouped-site', methods=['GET'])
def download_inventory_data_grouped_site():
    eng = create_engine(os.environ.get('DB_CONNECTION_STRING_READONLY'))

    # Query data for the General using Pandas
    general_query = """
        SELECT DISTINCT
            siteid,
            season,
            YEAR,
            sop,
            data_exists 
        FROM
            vw_qa_allsop_combined_final 
        ORDER BY
            siteid,
            season,
            YEAR,
            sop;
    """
    general_df = pd.read_sql(general_query, con=eng)

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
        "8": "SOP 8: Fish - BRUVs - Field",
        "8b": "SOP 8: Fish - BRUVs - Lab",
        "9": "SOP 9: Fish seines",
        "10": "SOP 10: Crab traps",
        "11": "SOP 11: Marsh plain vegetation and epifauna surveys",
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
    general_query = "SELECT * FROM mvw_qa_raw_logger_combined_final"
    general_df = pd.read_sql(general_query, con=eng)

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
        eng.execute("SELECT refresh_all_materialized_views();")
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
    df = pd.read_sql(query, eng)

    # Ensure that there is data for the requested parameter
    if df.empty:
        return jsonify({'error': 'No data found for the specified criteria'}), 404

    # Prepare the data in the format required for the graph (x: samplecollectiontimestamp, y: parameter value)
    graph_data = [{'x': row['samplecollectiontimestamp'], 'y': row[parameter]} for _, row in df.iterrows()]

    print(graph_data)


    # Return the graph data as JSON
    return jsonify(graph_data)




@admin.route('/get-sample-data', methods=['GET'])
def get_sample_data():

    # SOP to table mapping
    sop_choices = {
        "sop2": "tbl_waterquality_metadata",
        "sop3a": "tbl_sedchem_labbatch_data",
        "sop3b": "tbl_toxicitysummary",
        "sop4": "tbl_edna_metadata",
        "sop5": "tbl_sedgrainsize_labbatch_data",
        "sop6a": "tbl_benthicinfauna_labbatch",
        "sop6b": "tbl_benthiclarge_metadata",
        "sop7": "tbl_macroalgae_sample_metadata",
        "sop8": "tbl_bruv_metadata",
        "sop8b": "tbl_bruv_data",
        "sop9": "tbl_fish_sample_metadata",
        "sop10": "tbl_crabtrap_metadata",
        "sop11": "tbl_vegetation_sample_metadata",
        "sop13": "tbl_feldspar_metadata",
        "sop15": "tbl_trashsiteinfo"
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
    if sop_name == 'sop15':
        query = f"""
        SELECT sampledate, created_date
        FROM {table_name}
        WHERE siteid = :site_id
        AND sampledate >= :start_date
        AND sampledate <= :end_date
        ORDER BY sampledate;
        """
    else:
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
            if sop_name != 'sop15':
                samplecollectiondates = sorted(set(
                    row['samplecollectiondate'].strftime('%Y-%m-%d') for row in result if row['samplecollectiondate']
                ))
            else:
                samplecollectiondates = sorted(set(
                    row['sampledate'].strftime('%Y-%m-%d') for row in result if row['sampledate']
                ))
            created_dates = sorted(set(
                row['created_date'].strftime('%Y-%m-%d') for row in result if row['created_date']
            ))
            
            # Join the sorted dates into a comma-separated string
            return jsonify({
                'samplecollectiondate': ', '.join(samplecollectiondates) if samplecollectiondates else 'N/A',
                'created_date': ', '.join(created_dates) if created_dates else 'N/A'
            })
        else:
            return jsonify({'samplecollectiondate': 'N/A', 'created_date': 'N/A'})


    except Exception as e:
        print(e)
        return jsonify({'error': str(e)}), 500
    