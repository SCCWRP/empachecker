import os
import pandas as pd
from flask import current_app, Blueprint, render_template, g, send_file, request, make_response
from itertools import chain

from .core.functions import get_primary_key


def clean_data(value):
    if pd.isnull(value):  # Handle NaN
        return ""
    value = str(value)
    if value.startswith("="):  # Prevent formula interpretation
        return "'" + value
    return value

# dynamic lookup lists to template
templater = Blueprint('templater', __name__)
@templater.route('/templater', methods = ['GET', 'POST'])
def template():
    
    print("Begin Templater")
    
    eng = g.eng
    system_fields = current_app.system_fields
    datatype = request.args.get("datatype")
    template_info = current_app.datasets

    if datatype is None:
        return render_template("templates.jinja2", datasets=template_info, project_name = current_app.project_name)

    tables = template_info.get(datatype).get('tables')
    file_prefix = template_info.get(datatype).get('template_prefix')

    # get info lookup table
    lookup_sql = \
        """
            SELECT
                kcu.column_name, 
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name 
            FROM 
                information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' 
            AND tc.table_name IN ('{}')
            AND ccu.table_name LIKE 'lu_%%';
        """.format("','".join(tables))

    lookup_df = pd.read_sql(lookup_sql, eng)
    associated_lookup_lists = sorted(list(set(lookup_df.foreign_table_name)))

    # build glossary table
    glossary = pd.DataFrame()
    for tbl in tables:
        df = pd.read_sql(
            f"""
            SELECT
                cols.column_name as field_name,
                    cols.data_type as field_type,
                    (
                    SELECT
                        pg_catalog.col_description(c.oid, cols.ordinal_position::int)
                    FROM
                        pg_catalog.pg_class c
                    WHERE
                        c.oid = (SELECT ('"' || cols.table_name || '"')::regclass::oid)
                        AND c.relname = cols.table_name
                ) AS description
            FROM
                information_schema.columns cols
            WHERE
                cols.table_catalog    = 'empa2021'
                AND cols.column_name NOT IN ({','.join([f"'{x}'" for x in system_fields])})
                AND cols.table_name   = '{tbl}';
            
            """,
            eng
        )
        df = df.assign(
            sheet = pd.Series([tbl.replace("tbl_","") for _ in range(len(df))]),
            template_prefix = pd.Series([f"{file_prefix}-TEMPLATE" for _ in range(len(df))])
        )
        df = df[['template_prefix','sheet', 'field_name', 'field_type','description' ]]
        glossary = pd.concat([glossary, df],ignore_index=True)

    xls = {
        **{
            'Instructions': pd.DataFrame(
                
                    {
                        'How to use:': [
                            "Information about this spreadsheet:",
                            "SCCWRP spreadsheets follow a standard format, each consisting of several sheets: protocol metadata, sample metadata, sample data, and a glossary.",
                            "Metadata for each column can be found by selecting the header cell for that column, or in the glossary sheet. Please do not add or rename columns. Use note columns to provide additional information or context.",
                            "Questions or comments? Please contact Jan Walker at janw@sccwrp.org or Liesl Tiefenthaler at lieslt@sccwrp.org"
                        ]
                    }
                
            )
        },
        **{
            table.replace("tbl_", ""): pd.DataFrame(
                columns=
                [
                    *[
                        x for x in pd.read_sql(
                            """
                                SELECT {} FROM {} LIMIT 1
                            """.format(
                                ','.join(
                                    pd.read_sql(
                                        f"""
                                            SELECT 
                                                column_name 
                                            FROM 
                                                column_order 
                                            WHERE 
                                                table_name = '{table}'
                                            AND
                                                (column_name) IN 
                                                (
                                                    SELECT 
                                                        DISTINCT column_name 
                                                    FROM 
                                                        information_schema.columns 
                                                    WHERE 
                                                        table_name = '{table}' 
                                                ) 
                                            ORDER BY 
                                                custom_column_position 
                                        """, 
                                        eng
                                    ).column_name.tolist()),
                                table
                            ),
                            eng
                        ).columns.to_list()
                        if x not in system_fields
                    ]
                ]
            ) for table in tables
        },
        **{
            'glossary': glossary
        },
        **{
            lu_name: pd.read_sql(f"SELECT * from {lu_name}", eng)
            .drop(columns=['objectid'], errors='ignore')
            .applymap(clean_data)  # Apply cleaning to each cell
            for lu_name in associated_lookup_lists
        }
    }

    ######################################################################################################################################
    ########################################   START EXPORTING FORMATTING   ##############################################################
    ######################################################################################################################################
    all_pkeys = list(chain(*[get_primary_key(x, eng) for x in  tables]))
    all_fkeys = list(lookup_df.column_name)

    tables_str = ",".join([f"'{x}'" for x in tables])

    print(tables_str)
    qry = f"""
        SELECT A.COLUMN_NAME,
        (
            CASE
                
                WHEN b.lookup_list IS NOT NULL THEN A.column_comment || ' -Use lookup list: ' || b.lookup_list 
                ELSE A.column_comment
            END
        ) AS column_comment
        FROM
            (
            SELECT
                cols.TABLE_NAME AS TABLE_NAME,
                cols.COLUMN_NAME AS COLUMN_NAME,
                (
                SELECT
                    pg_catalog.col_description ( C.oid, cols.ordinal_position :: INT ) 
                FROM
                    pg_catalog.pg_class C 
                WHERE
                    C.oid = ( SELECT ( '"' || cols.TABLE_NAME || '"' ) :: regclass :: oid ) 
                    AND C.relname = cols.TABLE_NAME 
                ) AS column_comment 
            FROM
                information_schema.COLUMNS cols 
            WHERE
                cols.TABLE_NAME IN ( {tables_str} ) 
            GROUP BY
                cols.TABLE_NAME,
                cols.COLUMN_NAME,
                column_comment 
            )
            A LEFT JOIN (
            SELECT
                tc.TABLE_NAME,
                kcu.COLUMN_NAME,
                ccu.TABLE_NAME AS lookup_list 
            FROM
                information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME 
                AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME 
                AND ccu.table_schema = tc.table_schema 
            WHERE
                tc.constraint_type = 'FOREIGN KEY' 
                AND tc.TABLE_NAME IN ( {tables_str} ) 
                AND ccu.TABLE_NAME LIKE'lu_%%' 
            ) b ON A.TABLE_NAME = b.TABLE_NAME 
            AND A.COLUMN_NAME = b.COLUMN_NAME
    """
    
    print(qry)
    column_comment_df = pd.read_sql(qry,eng)
    
    # fill missing descriptions with N/A
    column_comment_df['column_comment'] = column_comment_df['column_comment'].fillna("N/A")
    column_comment = {x:y for x,y in zip(column_comment_df['column_name'], column_comment_df['column_comment'])}

    excel_file_path = f"{os.getcwd()}/export/{file_prefix}-TEMPLATE.xlsx"

    with pd.ExcelWriter(excel_file_path) as writer:
        workbook = writer.book
        
        # PRIMARY KEY COLUMNS format
        format_pkey = workbook.add_format({'bold': True, 'text_wrap': True})
        format_pkey.set_align('center')
        format_pkey.set_align('vcenter')
        format_pkey.set_rotation(90)

        # FOREIGN KEY COLUMNS format
        format_fkey = workbook.add_format({'bold': False, 'text_wrap': True, 'fg_color': '#D7D6D6'})
        format_fkey.set_align('center')
        format_fkey.set_align('vcenter')
        format_fkey.set_rotation(90)

        # BOTH PKEY AND FKEY
        format_pkey_fkey = workbook.add_format(
            {'bold': True, 'text_wrap': True, 'fg_color': '#D7D6D6'})
        format_pkey_fkey.set_align('center')
        format_pkey_fkey.set_align('vcenter')
        format_pkey_fkey.set_rotation(90)

        # OTHER COLUMNS format
        format_othercols = workbook.add_format({'bold': False, 'text_wrap': True})
        format_othercols.set_align('center')
        format_othercols.set_align('vcenter')
        format_othercols.set_rotation(90)

        # POPUP options
        options = {'font_size': 10, 'height': 200, 'width': 200}

        for sheet_pos, sheet_name in enumerate(xls.keys()):
            xls[sheet_name].to_excel(writer, sheet_name=sheet_name, index=False) 
            worksheet = writer.sheets[sheet_name]
            for col_num, col_name in enumerate(xls[sheet_name].columns.values):
                if (col_name in all_pkeys) and (col_name not in all_fkeys):
                    worksheet.write(0, col_num, col_name, format_pkey)
                    worksheet.write_comment(0, col_num, column_comment.get(col_name, 'N/A'), options)
                    worksheet.set_row(0, 170)
                elif (col_name in all_fkeys) and (col_name not in all_pkeys):
                    worksheet.write(0, col_num, col_name, format_fkey)
                    worksheet.write_comment(0, col_num, column_comment.get(col_name, 'N/A'), options)
                    worksheet.set_row(0, 170)
                elif (col_name in all_fkeys) and (col_name in all_pkeys):
                    worksheet.write(0, col_num, col_name, format_pkey_fkey)
                    worksheet.write_comment(0, col_num, column_comment.get(col_name, 'N/A'), options)
                    worksheet.set_row(0, 170)            
                else:
                    worksheet.write(0, col_num, col_name, format_othercols)
                    worksheet.write_comment(0, col_num, column_comment.get(col_name, 'N/A'), options)
                    worksheet.set_row(0, 170)      


    ######################################################################################################################################
    ########################################   END EXPORTING FORMATTING   ################################################################
    ######################################################################################################################################

    # Make a response object to set a custom cookie
    resp = make_response(send_file(f"{os.getcwd()}/export/{file_prefix}-TEMPLATE.xlsx", as_attachment=True, download_name=f'{file_prefix}-TEMPLATE.xlsx'))
    
    # Set a cookie to let browser know that the file has been sent
    resp.set_cookie('template_file_sent', 'true', max_age=1)

    print("End Templater")

    return resp




