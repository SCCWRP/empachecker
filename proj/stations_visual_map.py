from flask import render_template, request, jsonify, current_app, Blueprint, session, g, send_from_directory
from werkzeug.utils import secure_filename
from gc import collect
import os
import pandas as pd
import json
from pathlib import Path

map_check = Blueprint('map_check', __name__)
@map_check.route('/map/<submissionid>', methods=['GET'], strict_slashes=False)
def getmap(submissionid):
    return render_template(f'stations_visual_map.html', submissionid=submissionid)

get_map_info = Blueprint('get_map_info', __name__)
@get_map_info.route('/getmapinfo', methods = ['GET','POST'], strict_slashes=False)
def send_geojson():

    # Example path construction
    submission_id = session.get('submissionid', 'default')  
    path_to_points_json = os.path.join(os.getcwd(), "files", str(submission_id), "bad-points-geojson.json")
    path_to_polygons_json = os.path.join(os.getcwd(), "files", str(submission_id), "polygons-geojson.json")

    # If the JSON file exists, load it. Otherwise, return sample GeoJSON.
    if os.path.exists(path_to_points_json):
        with open(path_to_points_json, 'r') as f:
            sites = json.load(f)
    else:
        # Default example: One point
        sites = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {
                        "name": "Sample Point 1"
                    },
                    "geometry": {
                        "type": "Point",
                        # Example: near New York City
                        "coordinates": [-74.0060, 40.7128]
                    }
                }
            ]
        }

    if os.path.exists(path_to_polygons_json):
        with open(path_to_polygons_json, 'r') as f:
            catchments = json.load(f)
    else:
        # Default example: One polygon
        catchments = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {
                        "name": "Sample Polygon 1"
                    },
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [-74.009, 40.705],
                                [-74.001, 40.705],
                                [-74.001, 40.710],
                                [-74.009, 40.710],
                                [-74.009, 40.705]
                            ]
                        ]
                    }
                }
            ]
        }

    return jsonify(sites=sites, catchments=catchments)

  