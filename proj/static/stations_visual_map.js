 // 1. Initialize the map
const map = L.map('map').setView([37.5, -119.5], 6); // Centered on California

// 2. Add a tile layer (e.g., OpenStreetMap)
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  maxZoom: 19
}).addTo(map);

// 3. Create dropdown and add to the map
const dropdown = L.control({ position: 'topright' });
dropdown.onAdd = function (map) {
  const div = L.DomUtil.create('div', 'dropdown-container');
  div.innerHTML = `
    <select id="polygonSelector" style="padding: 5px; font-size: 14px;">
      <option value="">Loading...</option>
    </select>
  `;
  L.DomEvent.disableClickPropagation(div);
  return div;
};
dropdown.addTo(map);

// 4. Fetch your GeoJSON data from the backend
fetch('/empachecker/getmapinfo')
  .then(response => response.json())
  .then(data => {
    console.log("data:", data);
    
    // Check for core check: if siteid or stationno is undefined, show notification and exit further processing
    let coreCheckFailed = false;
    
    // Check sites data (if available)
    if (data.sites !== "None" && data.sites.features && data.sites.features.length > 0) {
      const firstSite = data.sites.features[0];
      if (typeof firstSite.properties.siteid === 'undefined' || typeof firstSite.properties.stationno === 'undefined') {
        coreCheckFailed = true;
      }
    }
    
    // Check catchments data (if available)
    if (data.catchments !== "None" && data.catchments.features && data.catchments.features.length > 0) {
      const firstCatchment = data.catchments.features[0];
      if (typeof firstCatchment.properties.siteid === 'undefined' || typeof firstCatchment.properties.stationno === 'undefined') {
        coreCheckFailed = true;
      }
    }
    
    if (coreCheckFailed) {
      L.popup({ closeOnClick: false, autoClose: false })
        .setLatLng(map.getCenter())
        .setContent("You need to pass Core Check before this check")
        .openOn(map);
      return; // Stop further processing
    }
    
    let polygonLayers = {}; // Store layers for easy access
    let firstPolygonKey = null; // Store the first polygon's key

    // Add point features (stations)
    if (data.sites !== "None") {
      L.geoJSON(data.sites, {
        pointToLayer: function(feature, latlng) {
          return L.circleMarker(latlng, {
            radius: 6,
            fillColor: "#0078FF",
            color: "#000",
            weight: 1,
            opacity: 1,
            fillOpacity: 0.8
          });
        },
        onEachFeature: function(feature, layer) {
          if (feature.properties) {
            const popupContent = `
              <b>Site ID:</b> ${feature.properties.siteid} <br>
              <b>Station No:</b> ${feature.properties.stationno} <br>
              <b>Latitude:</b> ${feature.properties.latitude} <br>
              <b>Longitude:</b> ${feature.properties.longitude} <br>
              <b>Row:</b> ${feature.properties.tmp_row + 2}
            `;
            layer.bindPopup(popupContent);
          }
        }
      }).addTo(map);
    }

    // Add polygon features (catchments)
    if (data.catchments !== "None") {
      const polygonLayer = L.geoJSON(data.catchments, {
        style: {
          color: '#FF0000', // Red outline
          weight: 2,
          opacity: 0.8,
          fillColor: "#FFAAAA", // Light red fill
          fillOpacity: 0.5
        },
        onEachFeature: function(feature, layer) {
          if (feature.properties) {
            const siteId = feature.properties.siteid;
            const stationNo = feature.properties.stationno;
            const key = `${siteId} - Station ${stationNo}`;

            // Store the first polygon key
            if (!firstPolygonKey) {
              firstPolygonKey = key;
            }

            // Store layer bounds
            polygonLayers[key] = layer;

            // Add popup
            layer.bindPopup(`<b>Site ID:</b> ${siteId} <br> <b>Station No:</b> ${stationNo}`);
          }
        }
      }).addTo(map);

      // Populate dropdown
      const selector = document.getElementById('polygonSelector');
      selector.innerHTML = ""; // Clear placeholder
      Object.keys(polygonLayers).forEach((key) => {
        const option = document.createElement("option");
        option.value = key;
        option.textContent = key;
        selector.appendChild(option);
      });

      // Automatically zoom to the first polygon
      if (firstPolygonKey && polygonLayers[firstPolygonKey]) {
        const bounds = polygonLayers[firstPolygonKey].getBounds();
        console.log('First polygon bounds:', bounds);
        map.fitBounds(bounds, { padding: [20, 20], maxZoom: 15 });
        selector.value = firstPolygonKey;
      }
      
      // Event listener for dropdown selection
      selector.addEventListener("change", function () {
        const selectedKey = this.value;
        if (polygonLayers[selectedKey]) {
          map.fitBounds(polygonLayers[selectedKey].getBounds());
        }
      });
    }
  })
  .catch(err => console.error('Error loading GeoJSON:', err));
