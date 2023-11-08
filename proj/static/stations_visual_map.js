require([
    "esri/config",
    "esri/Map",
    "esri/Graphic",
    "esri/views/MapView",
    "esri/layers/FeatureLayer",
    "esri/widgets/LayerList",
    "esri/widgets/Legend",
    "esri/layers/MapImageLayer",
    "esri/layers/GeoJSONLayer",
    "esri/Graphic",
    "esri/layers/GraphicsLayer",
    "esri/geometry/Polyline",
    "esri/geometry/Point",
    "esri/geometry/Polygon",
    "esri/widgets/Measurement",
    "esri/rest/support/Query"
], function(
    esriConfig, 
    Map, 
    Graphic, 
    MapView, 
    FeatureLayer, 
    LayerList, 
    Legend, 
    GeoJSONLayer, 
    MapImageLayer, 
    Graphic, 
    GraphicsLayer, 
    Polyline, 
    Point,
    Polygon,
    Measurement,
    Query
    ) {

    fetch(`/checker/getmapinfo`, {
        method: 'POST'
    }).then(function (response) 
        {return response.json()
    }).then(function (data) {

        const sitesData = data['sites']['features']
        const catchmentsData = data['catchments']['features']
        const arcGISAPIKey = data['arcgis_api_key']
        
        esriConfig.apiKey = arcGISAPIKey
        
        const map = new Map({
            basemap: "arcgis-topographic" // Basemap layer service
        });
    
        const view = new MapView({
            map: map,
            center: [-119.6638, 37.2153], //California
            zoom: 5,
            container: "viewDiv"
        });
        
        // Create a graphics layer
        const graphicsLayer = new GraphicsLayer();
        map.add(graphicsLayer);
        // Create a select element
        const siteSelect = document.createElement('select');
        siteSelect.id = 'zoomToSiteSelect';
        const defaultOption = document.createElement('option');
        defaultOption.value = '';
        defaultOption.textContent = 'Select a site to view:';
        siteSelect.appendChild(defaultOption);
        // Append the select element to the view's UI, for example in the top-right corner
        view.ui.add(siteSelect, 'top-right');


        // ////////////////// Ploting the sites //////////////////
        let simpleMarkerSymbol = {
            type: "simple-marker",
            color: [255, 0, 0],  // Red
            size: "15px",
            outline: {
                color: [255, 255, 255], // White
                width: 2
            }
        };

        if (sitesData !== 'None'){
            for (let i = 0; i < sitesData.length; i++){
                let coord = {
                    type: 'point',
                    longitude: sitesData[i]['geometry']['coordinates'][0],
                    latitude: sitesData[i]['geometry']['coordinates'][1]
                }
                let lat = sitesData[i]['properties']['latitude']
                let long = sitesData[i]['properties']['longitude']
                let tmp_row = parseInt(sitesData[i]['properties']['tmp_row']) + 2
                let siteid = sitesData[i]['properties']['siteid']

                let attr = {
                    lat: lat,
                    long: long,
                    tmp_row,
                    siteid
                };
                let popUp = {
                    title: "Points",
                    content: [
                        {
                            type: "fields",
                            fieldInfos: [
                                {
                                    fieldName: "lat", 
                                    label: "Latitude"
                                },
                                {
                                    fieldName: "long",
                                    label: "Longitude"
                                },
                                {
                                    fieldName: "tmp_row",
                                    label: "Row in Excel File"
                                },
                                {
                                    fieldName: "siteid",
                                    label: "Associated SiteID"
                                }
                            ]
                        }
                    ]
                };
                let pointGraphic = new Graphic({
                    geometry: coord,
                    symbol: simpleMarkerSymbol,
                    attributes: attr,
                    popupTemplate: popUp
                });
                graphicsLayer.add(pointGraphic);
            }
            sitesData.forEach((site, i) => {
                const option = document.createElement('option');
                option.value = i; // Index of the site
                option.textContent = `Row in Excel: ${parseInt(site['properties']['tmp_row']) + 2}`; // Text to show in the dropdown
                siteSelect.appendChild(option);
            });
            
            // Add the change event listener to the select element
            siteSelect.addEventListener('change', function() {
                const selectedSiteIndex = this.value;
                if (selectedSiteIndex) {
                    const selectedSite = sitesData[selectedSiteIndex];
                    // Zoom to the selected site's coordinates
                    view.goTo({
                        center: [
                            selectedSite['geometry']['coordinates'][0],
                            selectedSite['geometry']['coordinates'][1]
                        ],
                        zoom: 15 // Adjust zoom level as needed
                    });
                }
            });
        }
        // ////////////////////////////////////////////////////////////

        // ////////////////// Ploting the catchments //////////////////
        let simpleFillSymbol = {
            type: "simple-fill",
            color: [227, 139, 79, 0.2],  // Orange, opacity 80%
            size: "15px",
            outline: {
                color: [255, 255, 255],
                width: 1
            }
        };
        

        if (catchmentsData !== 'None'){
            for (let i = 0; i < catchmentsData.length; i++){
                let coord = {
                    type: 'polygon',
                    rings: catchmentsData[i]['geometry']['coordinates'][0]
                }
                let siteid = catchmentsData[i]['properties']['siteid']
                let attr = {
                    siteid: siteid
                };
                let popUp = {
                    title: "Sites",
                    content: [
                        {
                            type: "fields",
                            fieldInfos: [
                                {
                                    fieldName: "siteid"
                                }
                            ]
                        }
                    ]
                }
                var polygonGraphic  = new Graphic({
                    geometry: coord,
                    symbol: simpleFillSymbol,
                    attributes: attr,
                    popupTemplate: popUp
                });
                graphicsLayer.add(polygonGraphic);
            }

        }

        ////////////////////////////////////////////////////////////
    })
});