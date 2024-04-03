from flask import render_template, request, jsonify, current_app, Blueprint, session, g
from werkzeug.utils import secure_filename
from gc import collect
import os
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# custom imports, from local files
from .preprocess import clean_data
from .match import match
from .core.core import core
from .core.functions import fetch_meta
from .utils.functions import  check_elevation_fields
from .utils.generic import save_errors, correct_row_offset
from .utils.excel import mark_workbook
from .utils.exceptions import default_exception_handler
from .utils.reformat import parse_raw_logger_data
from .custom import *


upload = Blueprint('upload', __name__)
@upload.route('/upload',methods = ['GET','POST'])
def main():
    
    # -------------------------------------------------------------------------- #

    # First, the routine to upload the file(s)

    # routine to grab the uploaded file
    print("uploading files")
    files = request.files.getlist('files[]')
    if len(files) > 1:
        return jsonify(user_error_msg='You have submitted more than one file')
    elif len(files) == 0:
        return jsonify(user_error_msg="No file given")
    else:
        # i'd like to figure a way we can do it without writing the thing to an excel file
        # ... maybe
        # it would have to be written to a BytesIO object, which is completely possible, 
        #   but i think we might want to save the file to be able to give to them later
        f = files[0]
        filename = secure_filename(f.filename)
        file_extension = filename.lower().rsplit('.',1)[-1]

        # if file extension is xlsx/xls (hopefully xlsx)
        excel_path = os.path.join( session['submission_dir'], str(filename) )

        # the user's uploaded excel file can now be read into pandas
        f.save(excel_path)

        # To be accessed later by the upload routine that loads data to the tables
        session['excel_path'] = excel_path

        # Put their original filename in the submission tracking table
        g.eng.execute(
            f"""
            UPDATE submission_tracking_table 
            SET original_filename = '{filename}' 
            WHERE submissionid = {session.get('submissionid')};
            """
        )


    # We are assuming filename is an excel file or csv
    supported_checker_filetypes = ('xls','csv','txt','xlsx')
    if file_extension not in supported_checker_filetypes:
        errmsg = f"filename: {filename} is not a supported file type. Supported file types are {supported_checker_filetypes}"
        return jsonify(user_error_msg=errmsg)

    print("DONE uploading files")

    # -------------------------------------------------------------------------- #
    
    # Read in the excel file to make a dictionary of dataframes (all_dfs)

    # if they are not submitting logger_raw data, this if block shouldnt get executed, because session.get('login_info').get('login_filetype') should return None
    # raw-file or formatted-template are the two possible values (from config.json)
    print("session.get('login_info').get('login_filetype')")
    print(session.get('login_info').get('login_filetype')) 
    if session.get('login_info').get('login_filetype') == 'raw-file':
        print("Reformat")
        try:
            formatted_data = parse_raw_logger_data(session.get('login_info').get('login_sensortype'), session.get('excel_path'))
        except Exception as e:
            print(e)
            errmsg = f"Could not read raw logger file {filename}. Check the submission file type, and try again."
            return jsonify(user_error_msg=errmsg)
        
        # fill in the columns that should be populated from the data from the login form
        formatted_data = formatted_data.assign(
            projectid = session.get('login_info').get('login_project'),
            siteid = session.get('login_info').get('login_siteid'),
            estuaryname = session.get('login_info').get('login_siteid'),
            stationno = session.get('login_info').get('login_stationno'),
            sensortype = session.get('login_info').get('login_sensortype')
        )

        # replace siteids with estuary names in the estuary name column
        estuariesxwalk = pd.read_sql("SELECT siteid, estuary FROM lu_siteid", g.eng) \
            .set_index('siteid')['estuary'].to_dict()
        formatted_data.estuaryname = formatted_data.estuaryname.replace(estuariesxwalk)

        print("Done reformatting")
        all_dfs = {
            "tbl_wq_logger_raw": formatted_data
        }

        # Reset the excel path and filename
        filename = f"{str(filename)}.xlsx"
        excel_path = os.path.join( session['submission_dir'], filename )
        session['excel_path'] = excel_path
    
    elif (session.get('login_info').get('login_filetype') == 'formatted-template') and (Path(session.get('excel_path')).suffix != '.xlsx'):
        return jsonify(user_error_msg='Formatted Template was selected as filetype in the Session Login Information but you submitted a Raw File')
    else:
        assert isinstance(current_app.excel_offset, int), \
            "Number of rows to offset in excel file must be an integer. Check__init__.py"

        # build all_dfs where we will store their data
        print("building 'all_dfs' dictionary")
        all_dfs = {

            # Some projects may have descriptions in the first row, which are not the column headers
            # This is the only reason why the skiprows argument is used.
            # For projects that do not set up their data templates in this way, that arg should be removed

            # Note also that only empty cells will be regarded as missing values
            sheet: pd.read_excel(
                excel_path, 
                sheet_name = sheet,
                skiprows = current_app.excel_offset,
                na_values = [''],
                dtype={"amountoftrash": str},
                converters = {"preparationtime":str}
            )
            
            for sheet in pd.ExcelFile(excel_path).sheet_names
            
            if ((sheet not in current_app.tabs_to_ignore) and (not sheet.startswith('lu_')))
        }
    print(all_dfs)
    assert len(all_dfs) > 0, f"submissionid - {session.get('submissionid')} all_dfs is empty"
    
    for tblname in all_dfs.keys():
        all_dfs[tblname].columns = [x.lower() for x in all_dfs[tblname].columns]
        all_dfs[tblname] = all_dfs[tblname].drop(
            columns= [
                x 
                for x in all_dfs[tblname].columns 
                if x in current_app.system_fields
            ]
        )
    print("DONE - building 'all_dfs' dictionary")
    
    # Check if these sheets are not empty. I probably should put them in config.json
    sheets_to_check = current_app.data_required_sheets

    for sheet_name in sheets_to_check:
        if sheet_name in all_dfs.keys() and len(all_dfs[sheet_name]) == 0:
            return jsonify(user_error_msg=f'Please fill out the sheet {sheet_name} before you continue')

    # -------------------------------------------------------------------------- #

    # Match tables and dataset routine


    # alter the all_dfs variable with the match function
    # keys of all_dfs should be no longer the original sheet names but rather the table names that got matched, if any
    # if the tab didnt match any table it will not alter that item in the all_dfs dictionary
    print("Running match tables routine")
    match_dataset, match_report, all_dfs = match(all_dfs)
    
    print("match(all_dfs)")
    #print(match(all_dfs)) #uncommented to view

    #remember to comment out the block below after edits
    # print("match_dataset")
    # print(match_dataset)
    # print("match_report")
    # print(match_report)
    # print("all_dfs")
    # print(all_dfs)

    #NOTE if all tabs in all_dfs matched a database table, but there is still no match_dataset
    # then the problem probably lies in __init__.py

    # need an assert statement
    # an assert statement makes sense because in this would be an issue on our side rather than the user's



    print("DONE - Running match tables routine")

    session['datatype'] = match_dataset

    if match_dataset == "":
        # A tab in their excel file did not get matched with a table
        # return to user
        print("Failed to match a dataset")
        return jsonify(
            filename = filename,
            match_report = match_report,
            match_dataset = match_dataset,
            matched_all_tables = False
        )
    
    # If they made it this far, a dataset was matched
    g.eng.execute(
        f"""
        UPDATE submission_tracking_table 
        SET datatype = '{match_dataset}'
        WHERE submissionid = {session.get('submissionid')};
        """
    )



    # ----------------------------------------- #
    # Pre processing data before Core checks
    #  We want to limit the manual cleaning of the data that the user has to do
    #  This function will strip whitespace on character fields and fix columns to match lookup lists if they match (case insensitive)

    print("begin preprocessing")
    # We are not sure if we want to do this
    # some projects like bight prohibit this
    if match_dataset != 'logger_raw':
        # Skip preprocessing for raw logger data
        # We can probably add an option in the config on a per datatype basis to generalize this
        all_dfs = clean_data(all_dfs)
    print("end preprocessing ")
    # ----------------------------------------- #




    # write all_dfs again to the same excel path
    # Later, if the data is clean, the loading routine will use the tab names to load the data to the appropriate tables
    #   There is an assert statement (in load.py) which asserts that the tab names of the excel file match a table in the database
    #   With the way the code is structured, that should always be the case, but the assert statement will let us know if we messed up or need to fix something 
    #   Technically we could write it back with the original tab names, and use the tab_to_table_map in load.py,
    #   But for now, the tab_table_map is mainly used by the javascript in the front end, to display error messages to the user
    writer = pd.ExcelWriter(excel_path, engine = 'xlsxwriter', options = {"strings_to_formulas":False})
    for tblname in all_dfs.keys():
        all_dfs[tblname].to_excel(
            writer, 
            sheet_name = tblname, 
            startrow = current_app.excel_offset, 
            index=False
        )
    writer.save()
    
    # Yes this is weird but if we write the all_dfs back to the excel file, and read it back in,
    # this ensures 100% that the data is loaded exactly in the same state as it was in when it was checked
    all_dfs = {
        sheet: pd.read_excel(
            excel_path, 
            sheet_name = sheet,
            skiprows = current_app.excel_offset,
            na_values = [''],
            dtype={"amountoftrash": str}
        )
        for sheet in pd.ExcelFile(excel_path).sheet_names
        if ((sheet not in current_app.tabs_to_ignore) and (not sheet.startswith('lu_')))
    }

    
    # ----------------------------------------- #

    # Core Checks

    # initialize errors and warnings
    errs = []
    warnings = []

    print("Core Checks")

    # meta data is needed for the core checks to run, to check precision, length, datatypes, etc
    dbmetadata = {
        tblname: fetch_meta(tblname, g.eng)
        for tblname in set([y for x in current_app.datasets.values() for y in x.get('tables')])
    }

   
    # tack on core errors to errors list
    
    # debug = False will cause corechecks to run with multiprocessing, 
    # but the logs will not show as much useful information
    print("Right before core runs")
    core_output = core(all_dfs, g.eng, dbmetadata, debug = True)
    #core_output = core(all_dfs, g.eng, dbmetadata, debug = False)
    print("Right after core runs")

    errs.extend(core_output['core_errors'])
    warnings.extend(core_output['core_warnings'])

    # clear up some memory space, i never wanted to store the core checks output in memory anyways 
    # other than appending it to the errors/warnings list

    del core_output
    collect()
    
    
    
    print("DONE - Core Checks")



    # ----------------------------------------- #

    
    # Custom Checks based on match dataset

    assert match_dataset in current_app.datasets.keys(), \
        f"match dataset {match_dataset} not found in the current_app.datasets.keys()"


    # if there are no core errors, run custom checks

    # Users complain about this. 
    # However, often times, custom check functions make basic assumptions about the data,
    # which would depend on the data passing core checks. 
    
    # For example, it may assume that a certain column contains only numeric values, in order to check if the number
    # falls within an expected range of values, etc.
    # This makes the assumption that all values in that column are numeric, which is checked and enforced by Core Checks

    if errs == []: 
        print("Custom Checks")
        print(f"Datatype: {match_dataset}")

        # custom output should be a dictionary where errors and warnings are the keys and the values are a list of "errors" 
        # (structured the same way as errors are as seen in core checks section)
        
        # The custom checks function is stored in __init__.py in the datasets dictionary and accessed and called accordingly
        # match_dataset is a string, which should also be the same as one of the function names imported from custom, so we can "eval" it
        try:
            custom_output = eval(match_dataset)(all_dfs)

            print(f'Custom Output: {custom_output}')
            
            # Duy: We define global custom checks are the checks that apply to multiple datatypes.
            # the goal is to extend the custom output dictionnary
            # the output of global custom should look the same as the custom_output
            # Then we can do custom_output.get('errors').extend(global_custom_output.get('errors))
            # and custom_output.get('warnings').extend(global_custom_output.get('warnings))
            global_custom_output = global_custom(all_dfs, datatype = match_dataset)

            # extend the custom output
            custom_output.get('errors').extend(global_custom_output.get('errors'))
            custom_output.get('warnings').extend(global_custom_output.get('warnings'))
            
        except NameError as err:
            print("Error with custom checks")
            print(err)
            raise Exception(f"""Error calling custom checks function "{match_dataset}" - may not be defined, or was not imported correctly.""")
        except Exception as e:
            raise Exception(e)
        
        print("custom_output: ")
        print(custom_output)
        #example
        #map_output = current_app.datasets.get(match_dataset).get('map_function')(all_dfs)

        assert isinstance(custom_output, dict), \
            "custom output is not a dictionary. custom function is not written correctly"
        assert set(custom_output.keys()) == {'errors','warnings'}, \
            "Custom output dictionary should have keys which are only 'errors' and 'warnings'"

        # tack on errors and warnings
        # errs and warnings are lists initialized in the Core Checks section (above)
        errs.extend(custom_output.get('errors'))
        warnings.extend(custom_output.get('warnings'))

        errs = [e for e in errs if len(e) > 0]
        warnings = [w for w in warnings if len(w) > 0]


        print("DONE - Custom Checks")

    # End Custom Checks section    

    # Begin Visual Map Checks:

    # Run only if they passed Core Checks
    # if errs == []:
    #     # There are visual map checks for SAV, BRUV, Fish and Vegetation:

    #     map_func = current_app.datasets.get(match_dataset).get('map_func')
    #     if map_func is not None:
    #         map_output = map_func(all_dfs, current_app.datasets.get(match_dataset).get('spatialtable'))
    #         f = open(os.path.join(session.get('submission_dir'),f'{match_dataset}_map.html'),'w')
    #         f.write(map_output._repr_html_())
    #         f.close()

    # ---------------------------------------------------------------- #


    # Save the warnings and errors in the current submission directory
    # It would be convenient to store in the session cookie but that has a 4kb limit
    # instead we can just dump it to a json file
    save_errors(warnings, os.path.join( session['submission_dir'], "warnings.json" ))
    save_errors(errs, os.path.join( session['submission_dir'], "errors.json" ))
    # Later we will need to have a way to map the dataframe column names to the column indices
    # This is one of those lines of code where i dont know why it is here, but i have a feeling it will
    #   break things if i delete it
    # Even though i'm the one that put it here... -Robert <- classic programmer moment -Cas
    session['col_indices'] = {tbl: {col: df.columns.get_loc(col) for col in df.columns} for tbl, df in all_dfs.items() }

    # ---------------------------------------------------------------- #

    # By default the error and warnings collection methods assume that no rows were skipped in reading in of excel file.
    # It adds 1 to the row number when getting the error/warning, since excel is 1 based but the python dataframe indexing is zero based.
    # Therefore the row number in the errors and warnings will only match with their excel file's row if the column headers are actually in 
    #   the first row of the excel file.
    # These next few lines of code should correct that
    for e in errs: 
        assert type(e['rows']) == list, \
            "rows key in errs dict must be a list"
    errs = correct_row_offset(errs, offset = current_app.excel_offset)
    print("errs populated")
    warnings = correct_row_offset(warnings, offset = current_app.excel_offset)
    print("warnings populated")


    # -------------------------------------------------------------------------------- #

    # Mark up the excel workbook
    print("Marking Excel file")
    print(session.get('excel_path'))

    # mark_workbook function returns the file path to which it saved the marked excel file
    session['marked_excel_path'] = mark_workbook(
        all_dfs = all_dfs, 
        excel_path = session.get('excel_path'), 
        errs = errs, 
        warnings = warnings
    )

    print("DONE - Marking Excel file")

    # -------------------------------------------------------------------------------- #
    # Only display the map if lat, long are in the metadata
    if current_app.datasets.get(match_dataset).get('latlong_cols') is not None:
        station_visual_map = True
    else:
        station_visual_map = False
        
    # These are the values we are returning to the browser as a json
    returnvals = {
        "filename" : filename,
        "marked_filename" : f"{filename.rsplit('.',1)[0]}-marked.{filename.rsplit('.',1)[-1]}",
        "match_report" : match_report,
        "matched_all_tables" : True,
        "match_dataset" : match_dataset,
        "errs" : errs,
        "warnings": warnings,
        "submissionid": session.get("submissionid"),
        "critical_error": False,
        "all_datasets": list(current_app.datasets.keys()),
        "table_to_tab_map" : session['table_to_tab_map'],
        "has_visual_map": station_visual_map,
        "final_submit_requested": session.get('final_submit_requested', True),
        
        # to display the login email on the submission info tab
        "login_email": session.get("login_info", dict()).get('login_email', "non_existing_email@sccwrp.org") 
    }

    if match_dataset == 'logger_raw':
        jsondata = all_dfs['tbl_wq_logger_raw']
        jsondata['samplecollectiontimestamp'] = jsondata['samplecollectiontimestamp'].apply(lambda t: t.strftime("%Y-%m-%d %H:%M:%S") if pd.notnull(t) else '')
        plotcols = [
            'samplecollectiontimestamp',
            'samplecollectiontimezone',
            'raw_do',
            'raw_do_unit',
            'raw_do_pct',
            'raw_h2otemp',
            'raw_h2otemp_unit',
            'raw_conductivity',
            'raw_conductivity_unit',
            'raw_turbidity',
            'raw_turbidity_unit',
            'raw_salinity',
            'raw_salinity_unit',
            'raw_chlorophyll',
            'raw_chlorophyll_unit',
            'raw_pressure',
            'raw_pressure_unit'
        ]

        # for param in [p for p in plotcols if not ('unit' in p) or ('samplecollect' in p)]:
        #     # Create a line plot with a larger figure size
        #     plt.figure(figsize=(15, 10))
        #     plt.plot(jsondata['samplecollectiontimestamp'], jsondata[param])
        #     plt.title(f'Line Plot of {param} over time')
        #     plt.xlabel('samplecollectiontimestamp')
        #     ylabel = f"""{param} {','.join(jsondata[f'{param}_unit'].dropna().astype(str).unique())}""" if f'{param}_unit' in jsondata.columns else param
        #     plt.ylabel(ylabel)

        #     # Rotate x-axis labels
        #     plt.xticks(rotation=45)

        #     # Save the plot as a PNG file with a higher DPI
        #     plt.savefig(os.path.join(session.get('submission_dir'), f'{param}.png'), format='png', dpi=300, bbox_inches='tight')
        #     plt.close()

        returnvals['logger_data'] = jsondata[plotcols].fillna('').to_dict('records')

    #print(returnvals)

    print("DONE with upload routine, returning JSON to browser")
    return jsonify(**returnvals)


@upload.route('/map/<submissionid>/<datatype>')
def getmap(submissionid, datatype):
    datatype = str(datatype)
    if datatype not in ('sav','bruv','fishseines','vegetation'):
        return "Map not found"

    map_path = os.path.join(os.getcwd(), "files", str(submissionid), f'{datatype}_map.html')
    if os.path.exists(map_path):
        html = open(map_path,'r').read()
        return render_template(f'map_template.html', map=html)
    else:
        return "Map not found"



# When an exception happens when the browser is sending requests to the upload blueprint, this routine runs
@upload.errorhandler(Exception)
def upload_error_handler(error):
    response = default_exception_handler(
        mail_from = current_app.mail_from,
        errmsg = str(error),
        maintainers = current_app.maintainers,
        project_name = current_app.project_name,
        attachment = session.get('excel_path'),
        login_info = session.get('login_info'),
        submissionid = session.get('submissionid'),
        mail_server = current_app.config['MAIL_SERVER']
    )
    return response