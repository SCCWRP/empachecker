// Global variables
let map;
let estuaryLayers = [];
let stationLayers = [];
let allPolygonsData = {};
let estuaryGroups = {};
let sopStationData = {};
let sopMarkers = [];
let sopPolygonLayers = [];
let currentTab = 'estuary-tab';

// Initialize the map
function initMap() {
    map = L.map('map').setView([33.5, -117.8], 10); // Center on Southern California

    // Add Esri World Imagery satellite basemap
    L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
        attribution: 'Tiles © Esri — Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community',
        maxZoom: 19
    }).addTo(map);
}

// Show/hide loader
function showLoader() {
    document.getElementById('loader').classList.remove('hidden');
}

function hideLoader() {
    document.getElementById('loader').classList.add('hidden');
}

// Fetch polygon data from the backend
async function fetchPolygonData() {
    showLoader();
    try {
        const response = await fetch(`/${script_root}/get-all-polygons-data`);
        if (!response.ok) {
            throw new Error('Failed to fetch polygon data');
        }
        const data = await response.json();
        return data;
    } catch (error) {
        console.error('Error fetching polygon data:', error);
        alert('Failed to load station data. Please try again.');
        return { estuaries: [], stations: [] };
    } finally {
        hideLoader();
    }
}

// Add polygons to the map
function addPolygonsToMap(polygonsData) {
    // Clear existing layers
    estuaryLayers.forEach(layer => map.removeLayer(layer));
    stationLayers.forEach(layer => map.removeLayer(layer));
    estuaryLayers = [];
    stationLayers = [];
    estuaryGroups = {};

    // Initialize estuary groups for all stations first (even if no estuary boundary exists)
    const allEstuaryNames = new Set();
    polygonsData.stations.forEach(station => {
        allEstuaryNames.add(station.estuaryname);
        if (!estuaryGroups[station.estuaryname]) {
            estuaryGroups[station.estuaryname] = {
                estuaryLayer: null,
                stationLayers: []
            };
        }
    });

    // Add estuary polygons (RED) - these may not exist for all estuaries
    polygonsData.estuaries.forEach(estuary => {
        try {
            const geojson = JSON.parse(estuary.geometry);
            
            const layer = L.geoJSON(geojson, {
                style: {
                    color: '#ff0000',      // Red border
                    weight: 3,
                    opacity: 0.8,
                    fillColor: '#ff0000',  // Red fill
                    fillOpacity: 0.2
                }
            }).bindPopup(`
                <strong>Estuary:</strong> ${estuary.estuaryname}<br>
                <strong>Type:</strong> Estuary Boundary
            `);

            layer.addTo(map);
            estuaryLayers.push(layer);
            
            // Set estuary layer in the group
            if (estuaryGroups[estuary.estuaryname]) {
                estuaryGroups[estuary.estuaryname].estuaryLayer = layer;
            } else {
                estuaryGroups[estuary.estuaryname] = {
                    estuaryLayer: layer,
                    stationLayers: []
                };
            }
        } catch (e) {
            console.error(`Error parsing estuary geometry for ${estuary.estuaryname}:`, e);
        }
    });

    // Add station polygons (BLUE) - always plot these
    polygonsData.stations.forEach(station => {
        try {
            const geojson = JSON.parse(station.geometry);
            
            const layer = L.geoJSON(geojson, {
                style: {
                    color: '#0000ff',      // Blue border
                    weight: 2,
                    opacity: 0.8,
                    fillColor: '#0000ff',  // Blue fill
                    fillOpacity: 0.3
                }
            }).bindPopup(`
                <strong>Estuary:</strong> ${station.estuaryname}<br>
                <strong>Site ID:</strong> ${station.siteid}<br>
                <strong>Station No:</strong> ${station.stationno}<br>
                <strong>Type:</strong> Station
            `);

            layer.addTo(map);
            stationLayers.push(layer);
            
            // Add to estuary group (group already initialized above)
            estuaryGroups[station.estuaryname].stationLayers.push(layer);
        } catch (e) {
            console.error(`Error parsing station geometry for ${station.siteid}:`, e);
        }
    });
}

// Region-to-estuary mapping fetched from backend
let regionEstuaryMap = {};

// Fetch estuaries grouped by region from backend
async function fetchEstuariesByRegion() {
    try {
        const response = await fetch(`/${script_root}/get-estuaries-by-region`);
        if (!response.ok) {
            throw new Error('Failed to fetch estuaries by region');
        }
        const data = await response.json();
        regionEstuaryMap = data.regions || {};
    } catch (error) {
        console.error('Error fetching estuaries by region:', error);
        regionEstuaryMap = {};
    }
}

// Populate estuary dropdown based on selected region
function populateEstuaryDropdown(region) {
    const estuarySelect = document.getElementById('estuarySelect');
    const downloadBtn = document.getElementById('downloadShapefileBtn');

    estuarySelect.innerHTML = '';
    downloadBtn.disabled = true;

    if (!region || !regionEstuaryMap[region]) {
        estuarySelect.innerHTML = '<option value="">-- Select Region first --</option>';
        estuarySelect.disabled = true;
        return;
    }

    estuarySelect.disabled = false;
    estuarySelect.innerHTML = '<option value="">-- Select Estuary --</option>';

    const estuaries = regionEstuaryMap[region].sort();
    estuaries.forEach(estuary => {
        const option = document.createElement('option');
        option.value = estuary;
        option.textContent = estuary;
        estuarySelect.appendChild(option);
    });
}

// Populate estuary dropdown and checkboxes (kept for backward compat with map init)
function populateEstuaryControls(polygonsData) {
    // No-op: replaced by region/estuary cascading dropdown
}

// Zoom to selected estuaries (supports multiple selection)
function zoomToEstuaries(estuaryNames) {
    if (!estuaryNames || estuaryNames.length === 0) {
        // Reset to show all
        showAllPolygons();
        return;
    }

    // Hide all polygons first
    estuaryLayers.forEach(layer => map.removeLayer(layer));
    stationLayers.forEach(layer => map.removeLayer(layer));

    const bounds = L.latLngBounds([]);

    // Show selected estuaries and their stations
    estuaryNames.forEach(estuaryName => {
        const selectedGroup = estuaryGroups[estuaryName];
        if (selectedGroup) {
            // Add estuary boundary
            if (selectedGroup.estuaryLayer) {
                selectedGroup.estuaryLayer.addTo(map);
                bounds.extend(selectedGroup.estuaryLayer.getBounds());
            }

            // Add all stations for this estuary
            selectedGroup.stationLayers.forEach(layer => {
                layer.addTo(map);
                bounds.extend(layer.getBounds());
            });
        }
    });

    // Zoom to fit all polygons of selected estuaries
    if (bounds.isValid()) {
        map.fitBounds(bounds, { padding: [50, 50] });
    }
}

// Show all polygons
function showAllPolygons() {
    // Add all estuary layers
    estuaryLayers.forEach(layer => {
        if (!map.hasLayer(layer)) {
            layer.addTo(map);
        }
    });
    
    // Add all station layers
    stationLayers.forEach(layer => {
        if (!map.hasLayer(layer)) {
            layer.addTo(map);
        }
    });

    // Fit bounds to all polygons
    const allLayers = [...estuaryLayers, ...stationLayers];
    if (allLayers.length > 0) {
        const bounds = L.latLngBounds([]);
        allLayers.forEach(layer => {
            bounds.extend(layer.getBounds());
        });
        if (bounds.isValid()) {
            map.fitBounds(bounds, { padding: [50, 50] });
        }
    }
}

// Event listeners for region/estuary cascading dropdowns
document.getElementById('estuaryRegionSelect').addEventListener('change', (e) => {
    const selectedRegion = e.target.value;
    populateEstuaryDropdown(selectedRegion);
    // Reset map to show all when region changes
    showAllPolygons();
});

document.getElementById('estuarySelect').addEventListener('change', (e) => {
    const selectedEstuary = e.target.value;
    const downloadBtn = document.getElementById('downloadShapefileBtn');
    if (selectedEstuary) {
        zoomToEstuaries([selectedEstuary]);
        downloadBtn.disabled = false;
    } else {
        showAllPolygons();
        downloadBtn.disabled = true;
    }
});

document.getElementById('resetBtn').addEventListener('click', () => {
    document.getElementById('estuaryRegionSelect').value = '';
    document.getElementById('estuarySelect').value = '';
    document.getElementById('estuarySelect').disabled = true;
    document.getElementById('estuarySelect').innerHTML = '<option value="">-- Select Region first --</option>';
    document.getElementById('downloadShapefileBtn').disabled = true;
    showAllPolygons();
});

document.getElementById('downloadShapefileBtn').addEventListener('click', () => {
    const selectedEstuary = document.getElementById('estuarySelect').value;
    if (!selectedEstuary) {
        alert('Please select an estuary to download.');
        return;
    }
    let url = `/${script_root}/download-polygons-shapefile`;
    url += `?estuaries=${encodeURIComponent(selectedEstuary)}`;
    window.location.href = url;
});

// Tab switching functionality
function switchTab(tabName) {
    currentTab = tabName;
    
    // Update tab buttons
    document.querySelectorAll('.tab-button').forEach(btn => {
        btn.classList.remove('active');
        if (btn.dataset.tab === tabName) {
            btn.classList.add('active');
        }
    });
    
    // Update tab content visibility
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.remove('active');
    });
    
    document.querySelectorAll(`.${tabName}`).forEach(content => {
        content.classList.add('active');
    });
    
    // Clear map and reload appropriate data
    clearMap();
    
    if (tabName === 'estuary-tab') {
        showAllPolygons();
    } else if (tabName === 'sop-tab') {
        // SOP tab - load regions and wait for user to select region first
        document.getElementById('sopSelect').value = '';
        document.getElementById('sopSelect').disabled = true;
        populateRegionDropdown();
    }
}

function clearMap() {
    // Clear estuary/station layers
    estuaryLayers.forEach(layer => map.removeLayer(layer));
    stationLayers.forEach(layer => map.removeLayer(layer));
    
    // Clear SOP markers and polygons
    sopMarkers.forEach(marker => map.removeLayer(marker));
    sopPolygonLayers.forEach(layer => map.removeLayer(layer));
    sopMarkers = [];
    sopPolygonLayers = [];
}

// Fetch SOP station data
async function fetchSopStationData(tableName) {
    showLoader();
    try {
        const response = await fetch(`/${script_root}/get-sop-station-data?table=${tableName}`);
        if (!response.ok) {
            throw new Error('Failed to fetch SOP station data');
        }
        const data = await response.json();
        return data;
    } catch (error) {
        console.error('Error fetching SOP station data:', error);
        alert('Failed to load SOP station data. Please try again.');
        return { points: [], bad_points: [] };
    } finally {
        hideLoader();
    }
}

// Fetch all regions on page load
async function fetchAllRegions() {
    try {
        const response = await fetch(`/${script_root}/get-all-regions`);
        if (!response.ok) {
            throw new Error('Failed to fetch regions');
        }
        const data = await response.json();
        return data.regions;
    } catch (error) {
        console.error('Error fetching regions:', error);
        return [];
    }
}

// Populate region dropdown
async function populateRegionDropdown() {
    const regionSelect = document.getElementById('regionSelect');
    regionSelect.innerHTML = '<option value="">-- Select Region --</option>';
    
    const regions = await fetchAllRegions();
    regions.forEach(region => {
        const option = document.createElement('option');
        option.value = region;
        option.textContent = region;
        regionSelect.appendChild(option);
    });
}

// Handle region selection (enables SOP dropdown)
function handleRegionSelectionFirst(region) {
    const sopSelect = document.getElementById('sopSelect');
    
    if (!region) {
        // If no region selected, disable SOP
        sopSelect.disabled = true;
        sopSelect.value = '';
        clearMap();
        document.getElementById('badSiteSelect').disabled = true;
        return;
    }
    
    // Enable SOP dropdown when region is selected
    sopSelect.disabled = false;
    
    // If SOP is already selected, reload data with region filter
    if (sopSelect.value) {
        handleSopSelection(sopSelect.value);
    }
}

// Handle SOP selection
async function handleSopSelection(tableName) {
    if (!tableName) {
        clearMap();
        document.getElementById('badPointsTable').style.display = 'none';
        return;
    }
    
    showLoader();
    
    // Fetch station polygon data
    sopStationData = await fetchSopStationData(tableName);
    
    // Clear existing layers
    clearMap();
    
    // Add ALL station polygons to map (blue) - not just ones with data points
    await fetchPolygonData().then(polygonData => {
        polygonData.stations.forEach(station => {
            try {
                const geojson = JSON.parse(station.geometry);
                const layer = L.geoJSON(geojson, {
                    style: {
                        color: '#0000ff',
                        weight: 2,
                        opacity: 0.8,
                        fillColor: '#0000ff',
                        fillOpacity: 0.3
                    }
                }).bindPopup(`
                    <strong>Site Name:</strong> ${station.estuaryname || 'N/A'}<br>
                    <strong>SiteID:</strong> ${station.siteid || 'N/A'}<br>
                    <strong>Station No:</strong> ${station.stationno || 'N/A'}
                `);
                layer.addTo(map);
                sopPolygonLayers.push(layer);
            } catch (e) {
                console.error('Error parsing polygon geometry:', e);
            }
        });
    });
    
    // Add markers for bad points with color based on QA status
    sopStationData.bad_points.forEach(point => {
        // Determine color based on qa_action: green for confirm, yellow/orange for edit, red for none
        let markerColor = 'red';
        if (point.qa_action === 'confirm') {
            markerColor = '#28a745'; // Bootstrap success green
        } else if (point.qa_action === 'edit') {
            markerColor = '#ffc107'; // Bootstrap warning yellow
        }
        
        const marker = L.circleMarker([point.latitude, point.longitude], {
            radius: 6,
            color: markerColor,
            fillColor: markerColor,
            fillOpacity: 0.8
        });
        
        // Store the point data with the marker for later updates
        marker.pointData = point;
        
        const popupContent = `
            <div>
                <strong>ObjectID:</strong> ${point.objectid}<br>
                <strong>SiteID:</strong> ${point.siteid_meta}<br>
                <strong>Station (Meta):</strong> ${point.stationno_meta}<br>
                <strong>Station (Polygon):</strong> ${point.stationno_polygon || 'N/A'}<br>
                <strong>Status:</strong> ${point.match_status}<br>
                <strong>Sample Collection Date(s):</strong> ${point.samplecollectiondates || 'N/A'}<br>
                <strong>Lat:</strong> ${point.latitude.toFixed(6)}<br>
                <strong>Long:</strong> ${point.longitude.toFixed(6)}<br>
                ${point.qa_action ? `<strong>QA Status:</strong> ${point.qa_action}<br>` : ''}
                <div style="margin-top: 10px;">
                    <button class="btn btn-success btn-sm" onclick="handleConfirmPoint('${point.objectid}', '${point.siteid_meta}', '${point.region || ''}')">Confirm</button>
                    <button class="btn btn-warning btn-sm" onclick="handleEditPoint('${point.objectid}', '${point.siteid_meta}', '${point.region || ''}')">Edit</button>
                </div>
            </div>
        `;
        
        marker.bindPopup(popupContent);
        marker.addTo(map);
        sopMarkers.push(marker);
    });
    
    // Filter points by selected region
    const regionSelect = document.getElementById('regionSelect');
    const selectedRegion = regionSelect.value;
    
    if (selectedRegion) {
        sopStationData.bad_points = sopStationData.bad_points.filter(p => p.region === selectedRegion);
    }
    
    // Populate problematic sites dropdown (unique siteids)
    const badSiteSelect = document.getElementById('badSiteSelect');
    badSiteSelect.innerHTML = '<option value="">-- All problematic sites --</option>';
    
    const uniqueSites = [...new Set(sopStationData.bad_points.map(p => p.siteid_meta))].sort();
    uniqueSites.forEach(siteid => {
        const option = document.createElement('option');
        option.value = siteid;
        option.textContent = siteid;
        badSiteSelect.appendChild(option);
    });
    
    badSiteSelect.disabled = sopStationData.bad_points.length === 0;
    
    // Fit bounds to show all data
    const allLayers = [...sopPolygonLayers, ...sopMarkers];
    if (allLayers.length > 0) {
        const bounds = L.latLngBounds([]);
        allLayers.forEach(layer => {
            if (layer.getBounds) {
                bounds.extend(layer.getBounds());
            } else if (layer.getLatLng) {
                bounds.extend(layer.getLatLng());
            }
        });
        if (bounds.isValid()) {
            map.fitBounds(bounds, { padding: [50, 50] });
        }
    }
    
    hideLoader();
}

// Handle region selection for filtering (after SOP is loaded)
function handleRegionSelection(region) {
    const sopSelect = document.getElementById('sopSelect');
    
    // If SOP is selected, reload with region filter
    if (sopSelect.value) {
        handleSopSelection(sopSelect.value);
        return;
    }
    
    document.getElementById('badPointsTable').style.display = 'none';
    document.getElementById('badSiteSelect').value = '';
    
    // Filter sites by region
    const badSiteSelect = document.getElementById('badSiteSelect');
    badSiteSelect.innerHTML = '<option value="">-- All problematic sites --</option>';
    
    const filteredPoints = region 
        ? sopStationData.bad_points.filter(p => p.region === region)
        : sopStationData.bad_points;
    
    const uniqueSites = [...new Set(filteredPoints.map(p => p.siteid_meta))].sort();
    uniqueSites.forEach(siteid => {
        const option = document.createElement('option');
        option.value = siteid;
        option.textContent = siteid;
        badSiteSelect.appendChild(option);
    });
    
    // Clear and redraw markers based on region filter
    sopMarkers.forEach(marker => map.removeLayer(marker));
    sopMarkers = [];
    
    filteredPoints.forEach(point => {
        // Determine color based on qa_action
        let markerColor = 'red';
        if (point.qa_action === 'confirm') {
            markerColor = '#28a745'; // Bootstrap success green
        } else if (point.qa_action === 'edit') {
            markerColor = '#ffc107'; // Bootstrap warning yellow
        }
        
        const marker = L.circleMarker([point.latitude, point.longitude], {
            radius: 6,
            color: markerColor,
            fillColor: markerColor,
            fillOpacity: 0.8
        });
        
        // Store the point data with the marker
        marker.pointData = point;
        
        const popupContent = `
            <div>
                <strong>ObjectID:</strong> ${point.objectid}<br>
                <strong>SiteID:</strong> ${point.siteid_meta}<br>
                <strong>Station (Meta):</strong> ${point.stationno_meta}<br>
                <strong>Station (Polygon):</strong> ${point.stationno_polygon || 'N/A'}<br>
                <strong>Status:</strong> ${point.match_status}<br>
                <strong>Sample Collection Date(s):</strong> ${point.samplecollectiondates || 'N/A'}<br>
                <strong>Lat:</strong> ${point.latitude.toFixed(6)}<br>
                <strong>Long:</strong> ${point.longitude.toFixed(6)}<br>
                ${point.qa_action ? `<strong>QA Status:</strong> ${point.qa_action}<br>` : ''}
                <div style="margin-top: 10px;">
                    <button class="btn btn-success btn-sm" onclick="handleConfirmPoint('${point.objectid}', '${point.siteid_meta}', '${point.region || ''}')">Confirm</button>
                    <button class="btn btn-warning btn-sm" onclick="handleEditPoint('${point.objectid}', '${point.siteid_meta}', '${point.region || ''}')">Edit</button>
                </div>
            </div>
        `;
        
        marker.bindPopup(popupContent);
        marker.addTo(map);
        sopMarkers.push(marker);
    });
    
    // If a region is selected, zoom to show all points for that region
    if (region && filteredPoints.length > 0) {
        const bounds = L.latLngBounds(filteredPoints.map(p => [p.latitude, p.longitude]));
        map.fitBounds(bounds, { padding: [50, 50], maxZoom: 13 });
    }
}

// Handle confirm point action
async function handleConfirmPoint(objectids, siteid, region) {
    const userName = prompt('Please enter your name:');
    
    if (userName === null || userName.trim() === '') {
        alert('User name is required');
        return;
    }
    
    const sopSelect = document.getElementById('sopSelect');
    const sopTable = sopSelect.value;
    const sopText = sopSelect.options[sopSelect.selectedIndex].text;
    
    try {
        const response = await fetch(`/${script_root}/save-station-qa`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                sop: sopText,
                region: region,
                siteid: siteid,
                objectids: objectids,
                action: 'confirm',
                comment: '',
                last_edited_user: userName.trim()
            })
        });
        
        if (response.ok) {
            alert('Point confirmed successfully!');
            // Update marker color to green
            updateMarkerColor(objectids, siteid, '#28a745', 'confirm');
        } else {
            const error = await response.json();
            alert('Error confirming point: ' + (error.error || 'Unknown error'));
        }
    } catch (error) {
        console.error('Error confirming point:', error);
        alert('Error confirming point: ' + error.message);
    }
}

// Handle edit point action
async function handleEditPoint(objectids, siteid, region) {
    const userName = prompt('Please enter your name:');
    
    if (userName === null || userName.trim() === '') {
        alert('User name is required');
        return;
    }
    
    const comment = prompt('Please enter a comment for this edit:');
    
    if (comment === null) {
        return; // User cancelled
    }
    
    const sopSelect = document.getElementById('sopSelect');
    const sopTable = sopSelect.value;
    const sopText = sopSelect.options[sopSelect.selectedIndex].text;
    
    try {
        const response = await fetch(`/${script_root}/save-station-qa`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                sop: sopText,
                region: region,
                siteid: siteid,
                objectids: objectids,
                action: 'edit',
                comment: comment,
                last_edited_user: userName.trim()
            })
        });
        
        if (response.ok) {
            alert('Edit recorded successfully!');
            // Update marker color to yellow/orange
            updateMarkerColor(objectids, siteid, '#ffc107', 'edit');
        } else {
            const error = await response.json();
            alert('Error recording edit: ' + (error.error || 'Unknown error'));
        }
    } catch (error) {
        console.error('Error recording edit:', error);
        alert('Error recording edit: ' + error.message);
    }
}

// Update marker color after QA action
function updateMarkerColor(objectids, siteid, color, action) {
    // Find the marker that matches this objectids and siteid
    sopMarkers.forEach(marker => {
        if (marker.pointData && 
            marker.pointData.objectid === objectids && 
            marker.pointData.siteid_meta === siteid) {
            // Update the marker style
            marker.setStyle({
                color: color,
                fillColor: color,
                fillOpacity: 0.8
            });
            // Update the point data
            marker.pointData.qa_action = action;
            
            // Update the popup content
            const point = marker.pointData;
            const popupContent = `
                <div>
                    <strong>ObjectID:</strong> ${point.objectid}<br>
                    <strong>SiteID:</strong> ${point.siteid_meta}<br>
                    <strong>Station (Meta):</strong> ${point.stationno_meta}<br>
                    <strong>Station (Polygon):</strong> ${point.stationno_polygon || 'N/A'}<br>
                    <strong>Status:</strong> ${point.match_status}<br>
                    <strong>Sample Collection Date(s):</strong> ${point.samplecollectiondates || 'N/A'}<br>
                    <strong>Lat:</strong> ${point.latitude.toFixed(6)}<br>
                    <strong>Long:</strong> ${point.longitude.toFixed(6)}<br>
                    <strong>QA Status:</strong> ${action}<br>
                    <div style="margin-top: 10px;">
                        <button class="btn btn-success btn-sm" onclick="handleConfirmPoint('${point.objectid}', '${point.siteid_meta}', '${point.region || ''}')">Confirm</button>
                        <button class="btn btn-warning btn-sm" onclick="handleEditPoint('${point.objectid}', '${point.siteid_meta}', '${point.region || ''}')">Edit</button>
                    </div>
                </div>
            `;
            marker.setPopupContent(popupContent);
        }
    });
}

// Handle bad site selection
function handleBadSiteSelection(siteid) {
    document.getElementById('badPointsTable').style.display = 'none';
    
    const regionSelect = document.getElementById('regionSelect');
    const selectedRegion = regionSelect.value;
    
    // If a site is selected, zoom to show all points for that site
    if (siteid) {
        let filteredPoints = sopStationData.bad_points.filter(p => p.siteid_meta === siteid);
        
        // Also apply region filter if selected
        if (selectedRegion) {
            filteredPoints = filteredPoints.filter(p => p.region === selectedRegion);
        }
        
        if (filteredPoints.length > 0) {
            const bounds = L.latLngBounds(filteredPoints.map(p => [p.latitude, p.longitude]));
            map.fitBounds(bounds, { padding: [50, 50], maxZoom: 13 });
        }
    }
}

// Track if SOP tab is authenticated
let sopTabAuthenticated = false;

// SOP link click handler
document.getElementById('sopLink').addEventListener('click', (e) => {
    e.preventDefault();

    // Require password for SOP Verification
    if (!sopTabAuthenticated) {
        const password = prompt('Enter password to access SOP Verification:');
        if (password === '3535$Harbor') {
            sopTabAuthenticated = true;
            switchTab('sop-tab');
        } else if (password !== null) {
            alert('Incorrect password.');
        }
        return;
    }

    switchTab('sop-tab');
});

// Back to Estuary link click handler
document.getElementById('backToEstuary').addEventListener('click', (e) => {
    e.preventDefault();
    switchTab('estuary-tab');
});

// Event listeners for SOP tab
document.getElementById('sopSelect').addEventListener('change', (e) => {
    handleSopSelection(e.target.value);
});

document.getElementById('regionSelect').addEventListener('change', (e) => {
    if (currentTab === 'sop-tab') {
        handleRegionSelectionFirst(e.target.value);
    }
});

document.getElementById('badSiteSelect').addEventListener('change', (e) => {
    handleBadSiteSelection(e.target.value);
});

document.getElementById('resetSopBtn').addEventListener('click', () => {
    document.getElementById('sopSelect').value = '';
    document.getElementById('sopSelect').disabled = true;
    document.getElementById('regionSelect').value = '';
    document.getElementById('badSiteSelect').value = '';
    document.getElementById('badSiteSelect').disabled = true;
    document.getElementById('badPointsTable').style.display = 'none';
    clearMap();
});

// Toggle panel functions
function togglePanel(panel, button) {
    panel.classList.toggle('minimized');
    const isMinimized = panel.classList.contains('minimized');
    if (button) {
        button.innerHTML = isMinimized ? '&#9660;' : '&#9650;'; // Down arrow / Up arrow
        button.title = isMinimized ? 'Expand' : 'Minimize';
    }
}

// Open this map in a new browser tab
document.getElementById('viewMapNewTabBtn').addEventListener('click', (e) => {
    e.stopPropagation();
    window.open(window.location.href, '_blank');
});

// Make the estuary header clickable to expand panel and open dropdown
document.getElementById('estuaryHeader').addEventListener('click', () => {
    const panel = document.getElementById('controls');
    const wasMinimized = panel.classList.contains('minimized');
    // Always expand if minimized
    if (wasMinimized) {
        togglePanel(panel);
    }
    // After expanding, open the appropriate dropdown
    setTimeout(() => {
        const regionSelect = document.getElementById('estuaryRegionSelect');
        const estuarySelect = document.getElementById('estuarySelect');
        try {
            if (!regionSelect.value) {
                regionSelect.showPicker();
            } else if (!estuarySelect.disabled) {
                estuarySelect.showPicker();
            }
        } catch(e) {
            if (!regionSelect.value) {
                regionSelect.focus();
            } else if (!estuarySelect.disabled) {
                estuarySelect.focus();
            }
        }
    }, 50);
});

document.getElementById('toggleSopControls').addEventListener('click', () => {
    togglePanel(document.getElementById('sopControls'), document.getElementById('toggleSopControls'));
});

// Initialize on page load
document.addEventListener('DOMContentLoaded', async () => {
    initMap();

    // Fetch region-estuary mapping and polygon data in parallel
    const [polygonsData] = await Promise.all([
        fetchPolygonData(),
        fetchEstuariesByRegion()
    ]);
    allPolygonsData = polygonsData;

    if (allPolygonsData.estuaries.length > 0 || allPolygonsData.stations.length > 0) {
        addPolygonsToMap(allPolygonsData);
        showAllPolygons();
    } else {
        alert('No station data available.');
    }
});
