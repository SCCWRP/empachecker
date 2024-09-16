import os, time, re
import pandas as pd
from bs4 import BeautifulSoup
from flask import Blueprint, g, current_app, render_template, redirect, url_for, session, request, jsonify, render_template_string, send_file
from io import StringIO
import psycopg2
from psycopg2 import sql

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

    # if request.method == 'POST':
    #     print("report requested")
    #     startdate = request.form.get('startdate')
    #     enddate = request.form.get('enddate')
    #     requested_sites = request.form.getlist('siteid[]')
    #     print(requested_sites)
        
    #     if not startdate:
    #         return "You did not select start date"
    #     if not enddate:
    #         return "You did not select end date"
    #     if requested_sites != '':
    #         site_list = [f"'{site.strip()}'" for site in requested_sites]
    #         formatted_site_list = ", ".join(site_list)

    #     if startdate and enddate and len(requested_sites) == 0:
    #         # Fetch data based on provided dates.
    #         qry = f"""
    #             SELECT
    #             projectid,
    #             siteid,
    #             estuaryname,
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_wq_logger_metadata
    #                     WHERE
    #                         tbl_wq_logger_metadata.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiontimestampstart BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "SOP 1 - logger",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_waterquality_metadata
    #                     WHERE
    #                         tbl_waterquality_metadata.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiondate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "SOP 2 - discrete",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_edna_metadata
    #                     WHERE
    #                         tbl_edna_metadata.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiondate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "SOP 4 - eDNA -field",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_grabevent
    #                     WHERE
    #                         tbl_grabevent.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiondate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "Grab Table",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_sedchem_data
    #                     WHERE
    #                         tbl_sedchem_data.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiondate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "Sop 3 - SedimentChemistry - lab",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_sedgrainsize_data
    #                     WHERE
    #                         tbl_sedgrainsize_data.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiondate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "Sop 5 - GS - lab",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_benthicinfauna_labbatch
    #                     WHERE
    #                         tbl_benthicinfauna_labbatch.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiondate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "SOP 6 - infauna - lab",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_benthiclarge_metadata
    #                     WHERE
    #                         tbl_benthiclarge_metadata.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiondate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "SOP 6 - infauna - large",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_macroalgae_sample_metadata
    #                     WHERE
    #                         tbl_macroalgae_sample_metadata.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiondate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "SOP 7 - macroalgae",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_bruv_metadata
    #                     WHERE
    #                         tbl_bruv_metadata.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiondate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "SOP 8 - BRUVs - field",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_bruv_data
    #                     WHERE
    #                         tbl_bruv_data.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiondate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "SOP 8 - BRUVs - lab",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_fish_sample_metadata
    #                     WHERE
    #                         tbl_fish_sample_metadata.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiondate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "SOP 9 - Fishes",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_crabtrap_metadata
    #                     WHERE
    #                         tbl_crabtrap_metadata.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiondate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "SOP 10 - Crabs",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_vegetation_sample_metadata
    #                     WHERE
    #                         tbl_vegetation_sample_metadata.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiondate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "SOP 11 - Vegetation",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_feldspar_metadata
    #                     WHERE
    #                         tbl_feldspar_metadata.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiondate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "SOP 13 - Feldspar",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_trashsiteinfo
    #                     WHERE
    #                         tbl_trashsiteinfo.siteid = sample_assignment_info.siteid
    #                         AND sampledate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "SOP 15 - Trash"
    #         FROM
    #             sample_assignment_info;
    #         """
    #     elif startdate and enddate and len(requested_sites) > 0:
    #         qry = f"""
    #             SELECT
    #             projectid,
    #             siteid,
    #             estuaryname,
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_wq_logger_metadata
    #                     WHERE
    #                         tbl_wq_logger_metadata.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiontimestampstart BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "SOP 1 - logger",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_waterquality_metadata
    #                     WHERE
    #                         tbl_waterquality_metadata.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiondate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "SOP 2 - discrete",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_edna_metadata
    #                     WHERE
    #                         tbl_edna_metadata.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiondate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "SOP 4 - eDNA -field",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_grabevent
    #                     WHERE
    #                         tbl_grabevent.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiondate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "Grab Table",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_sedchem_data
    #                     WHERE
    #                         tbl_sedchem_data.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiondate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "Sop 3 - SedimentChemistry - lab",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_sedgrainsize_data
    #                     WHERE
    #                         tbl_sedgrainsize_data.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiondate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "Sop 5 - GS - lab",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_benthicinfauna_labbatch
    #                     WHERE
    #                         tbl_benthicinfauna_labbatch.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiondate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "SOP 6 - infauna - lab",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_benthiclarge_metadata
    #                     WHERE
    #                         tbl_benthiclarge_metadata.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiondate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "SOP 6 - infauna - large",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_macroalgae_sample_metadata
    #                     WHERE
    #                         tbl_macroalgae_sample_metadata.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiondate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "SOP 7 - macroalgae",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_bruv_metadata
    #                     WHERE
    #                         tbl_bruv_metadata.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiondate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "SOP 8 - BRUVs - field",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_bruv_data
    #                     WHERE
    #                         tbl_bruv_data.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiondate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "SOP 8 - BRUVs - lab",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_fish_sample_metadata
    #                     WHERE
    #                         tbl_fish_sample_metadata.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiondate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "SOP 9 - Fishes",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_crabtrap_metadata
    #                     WHERE
    #                         tbl_crabtrap_metadata.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiondate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "SOP 10 - Crabs",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_vegetation_sample_metadata
    #                     WHERE
    #                         tbl_vegetation_sample_metadata.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiondate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "SOP 11 - Vegetation",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_feldspar_metadata
    #                     WHERE
    #                         tbl_feldspar_metadata.siteid = sample_assignment_info.siteid
    #                         AND samplecollectiondate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "SOP 13 - Feldspar",
    #             CASE
    #                 WHEN (
    #                     SELECT
    #                         COUNT(*)
    #                     FROM
    #                         tbl_trashsiteinfo
    #                     WHERE
    #                         tbl_trashsiteinfo.siteid = sample_assignment_info.siteid
    #                         AND sampledate BETWEEN '{startdate}' AND '{enddate}'
    #                 ) > 0 THEN 'X'
    #                 ELSE NULL
    #             END AS "SOP 15 - Trash"
    #         FROM
    #             sample_assignment_info
    #         WHERE sample_assignment_info.siteid IN ({formatted_site_list});
    #         """
    #     print(qry)

    #     df = pd.read_sql(qry, g.eng)
    #     df = df.sort_values(by=['projectid', 'siteid'])
    #     df.to_csv(os.path.join(os.getcwd(), 'export','inventory-report.csv'),index=False)
    #     # Convert DataFrame to HTML
    #     df_html = df.to_html(index=False)
    #     # Parse the HTML to modify it.
    #     soup = BeautifulSoup(df_html, "html.parser")

    #     # Iterate over each table cell.
    #     for cell in soup.find_all("td"):
    #         if cell.get_text().strip() == "X":
    #             cell["style"] = "background-color: #CCFFCC; color: green;"
    #         elif cell.get_text().strip() == "None":
    #             cell["style"] = "background-color: #FF9999"

    #     # Convert the modified HTML back to a string.
    #     modified_html = str(soup)

    #     # Return the report page with the modified table and a button.
    #     return render_template_string(f"""
    #     <html>
    #     <head>
    #         <title>Report</title>
    #     </head>
    #     <body>
    #         <button onclick="window.location.href='/checker/report';">Generate New Report</button>
    #         <button onclick="window.location.href='/checker/report-download';">Download CSV</button>                          
    #         <h1>Report</h1>
    #         <p>Selected Date Range: {startdate} to {enddate}</p>
    #         {modified_html}
    #     </body>
    #     </html>
    #     """)

    # # For GET requests or if no dates are provided, render the initial form.
    # siteids = pd.read_sql("SELECT DISTINCT siteid from lu_siteid ORDER BY siteid", g.eng).siteid.tolist()
    # return render_template("report.html", siteids = siteids)


@admin.route('/report-download', methods=['GET', 'POST'])
def report_download():
    # Return as a downloadable file
    return send_file(os.path.join(os.getcwd(), 'export','inventory-report.csv'), as_attachment=True, download_name="report.csv", mimetype="text/csv")
