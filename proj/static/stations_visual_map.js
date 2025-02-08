// stations_visual_map.js

// 1. Initialize the map
const map = L.map('map').setView([40, -95], 4); // center on the US, zoom level 4 (example)

// 2. Add a tile layer (e.g., OpenStreetMap)
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  maxZoom: 19
}).addTo(map);

// 3. Fetch your geojson data from the backend
fetch('/empachecker/getmapinfo')
  .then(response => response.json())
  .then(data => {
    // data.sites is presumably a GeoJSON for points
    // data.catchments is presumably a GeoJSON for polygons

    // Check if data.sites is valid
    if (data.sites !== "None") {
      L.geoJSON(data.sites, {
        // Example style, popup, etc.
        onEachFeature: function(feature, layer) {
          layer.bindPopup(`Point: ${feature.properties.name || 'No name'}`);
        }
      }).addTo(map);
    }

    // Check if data.catchments is valid
    if (data.catchments !== "None") {
      L.geoJSON(data.catchments, {
        style: {
          color: '#FF0000',
          weight: 2,
          opacity: 0.8
        }
      }).addTo(map);
    }
  })
  .catch(err => console.error('Error loading GeoJSON:', err));
