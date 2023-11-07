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
        for (let i = 0; i < sitesData.length; i++){
            let coord = {
                type: 'point',
                longitude: sitesData[i]['geometry']['coordinates'][0],
                latitude: sitesData[i]['geometry']['coordinates'][1]
            }
            let siteid = sitesData[i]['properties']['siteid']
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
            let pointGraphic = new Graphic({
                geometry: coord,
                symbol: simpleMarkerSymbol,
                attributes: attr,
                popupTemplate: popUp
            });
            graphicsLayer.add(pointGraphic);
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
                title: "Catchments",
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
        ////////////////////////////////////////////////////////////
    })
});