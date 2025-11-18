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

    // Add OpenStreetMap tiles
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '© OpenStreetMap contributors',
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

// Populate estuary dropdown
function populateEstuaryDropdown(polygonsData) {
    const estuarySelect = document.getElementById('estuarySelect');
    
    // Get unique estuary names from BOTH estuaries and stations
    const estuariesFromBoundaries = new Set(polygonsData.estuaries.map(e => e.estuaryname));
    const estuariesFromStations = new Set(polygonsData.stations.map(s => s.estuaryname));
    
    // Combine both sets to get all unique estuary names
    const allEstuaries = new Set([...estuariesFromBoundaries, ...estuariesFromStations]);
    const estuaries = [...allEstuaries].sort();

    // Clear existing options (except the first "All" option)
    estuarySelect.innerHTML = '<option value="">-- All Estuaries --</option>';

    estuaries.forEach(estuary => {
        const option = document.createElement('option');
        option.value = estuary;
        option.textContent = estuary;
        estuarySelect.appendChild(option);
    });
}

// Zoom to selected estuary
function zoomToEstuary(estuaryName) {
    if (!estuaryName) {
        // Reset to show all
        showAllPolygons();
        return;
    }

    // Hide all polygons first
    estuaryLayers.forEach(layer => map.removeLayer(layer));
    stationLayers.forEach(layer => map.removeLayer(layer));

    // Show only selected estuary and its stations
    const selectedGroup = estuaryGroups[estuaryName];
    if (selectedGroup) {
        const bounds = L.latLngBounds([]);
        
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
        
        // Zoom to fit all polygons of selected estuary
        if (bounds.isValid()) {
            map.fitBounds(bounds, { padding: [50, 50] });
        }
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

// Event listeners
document.getElementById('estuarySelect').addEventListener('change', (e) => {
    zoomToEstuary(e.target.value);
});

document.getElementById('resetBtn').addEventListener('click', () => {
    document.getElementById('estuarySelect').value = '';
    showAllPolygons();
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
        // SOP tab - wait for user to select SOP
        document.getElementById('sopSelect').value = '';
        document.getElementById('badPointSelect').disabled = true;
        document.getElementById('downloadBadPoints').disabled = true;
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

// Handle SOP selection
async function handleSopSelection(tableName) {
    if (!tableName) {
        clearMap();
        document.getElementById('badPointSelect').disabled = true;
        document.getElementById('downloadBadPoints').disabled = true;
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
    
    // Add markers for bad points (red)
    sopStationData.bad_points.forEach(point => {
        const marker = L.circleMarker([point.latitude, point.longitude], {
            radius: 6,
            color: 'red',
            fillColor: 'red',
            fillOpacity: 0.8
        }).bindPopup(`
            <strong>ObjectID:</strong> ${point.objectid}<br>
            <strong>Site Name:</strong> ${point.sitename || 'N/A'}<br>
            <strong>SiteID:</strong> ${point.siteid_meta}<br>
            <strong>Station (Meta):</strong> ${point.stationno_meta}<br>
            <strong>Station (Polygon):</strong> ${point.stationno_polygon || 'N/A'}<br>
            <strong>Status:</strong> ${point.match_status}<br>
            <strong>Lat:</strong> ${point.latitude.toFixed(6)}<br>
            <strong>Long:</strong> ${point.longitude.toFixed(6)}
        `);
        marker.addTo(map);
        sopMarkers.push(marker);
    });
    
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
    
    // Initially populate all bad points
    populateBadPointsDropdown('');
    
    document.getElementById('downloadBadPoints').disabled = sopStationData.bad_points.length === 0;
    
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

// Populate bad points dropdown based on selected site
function populateBadPointsDropdown(siteid) {
    const badPointSelect = document.getElementById('badPointSelect');
    badPointSelect.innerHTML = '<option value="">-- Select point --</option>';
    
    const filteredPoints = siteid 
        ? sopStationData.bad_points.filter(p => p.siteid_meta === siteid)
        : sopStationData.bad_points;
    
    filteredPoints.forEach(point => {
        const idx = sopStationData.bad_points.indexOf(point);
        const option = document.createElement('option');
        option.value = idx;
        option.textContent = `Station ${point.stationno_meta} - Lat: ${point.latitude.toFixed(6)}, Long: ${point.longitude.toFixed(6)}`;
        badPointSelect.appendChild(option);
    });
    
    badPointSelect.disabled = filteredPoints.length === 0;
    
    // If a site is selected, zoom to show all points for that site
    if (siteid && filteredPoints.length > 0) {
        const bounds = L.latLngBounds(filteredPoints.map(p => [p.latitude, p.longitude]));
        map.fitBounds(bounds, { padding: [50, 50], maxZoom: 13 });
    }
}

// Handle bad site selection
function handleBadSiteSelection(siteid) {
    document.getElementById('badPointsTable').style.display = 'none';
    document.getElementById('badPointSelect').value = '';
    populateBadPointsDropdown(siteid);
}

// Handle bad point selection
function handleBadPointSelection(idx) {
    if (idx === '') {
        document.getElementById('badPointsTable').style.display = 'none';
        return;
    }
    
    const point = sopStationData.bad_points[idx];
    
    // Zoom to selected point
    map.setView([point.latitude, point.longitude], 15);
    
    // Open popup for the marker
    sopMarkers[idx].openPopup();
    
    // Show details table
    const tableBody = document.getElementById('badPointTableBody');
    tableBody.innerHTML = `
        <tr><th>ObjectID</th><td>${point.objectid}</td></tr>
        <tr><th>Site Name</th><td>${point.sitename || 'N/A'}</td></tr>
        <tr><th>SiteID (Meta)</th><td>${point.siteid_meta}</td></tr>
        <tr><th>Station (Meta)</th><td>${point.stationno_meta}</td></tr>
        <tr><th>Station (Polygon)</th><td>${point.stationno_polygon || 'N/A'}</td></tr>
        <tr><th>Match Status</th><td>${point.match_status}</td></tr>
        <tr><th>Latitude</th><td>${point.latitude.toFixed(6)}</td></tr>
        <tr><th>Longitude</th><td>${point.longitude.toFixed(6)}</td></tr>
    `;
    
    document.getElementById('badPointsTable').style.display = 'block';
}

// Download bad points as CSV
function downloadBadPointsCSV() {
    if (!sopStationData.bad_points || sopStationData.bad_points.length === 0) {
        alert('No mismatched points to download');
        return;
    }
    
    const sopSelect = document.getElementById('sopSelect');
    const selectedText = sopSelect.options[sopSelect.selectedIndex].text;
    
    // Create CSV content
    let csv = 'ObjectID,Site Name,SiteID (Meta),Latitude,Longitude,Station (Meta),Station (Polygon),Match Status\n';
    
    sopStationData.bad_points.forEach(point => {
        csv += `${point.objectid},${point.sitename || 'N/A'},${point.siteid_meta},${point.latitude.toFixed(6)},${point.longitude.toFixed(6)},${point.stationno_meta},${point.stationno_polygon || 'N/A'},${point.match_status}\n`;
    });
    
    // Create download link
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${selectedText}-polygon-check-report.csv`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    window.URL.revokeObjectURL(url);
}

// Event listeners for tab switching
document.querySelectorAll('.tab-button').forEach(btn => {
    btn.addEventListener('click', (e) => {
        switchTab(e.target.dataset.tab);
    });
});

// Event listeners for SOP tab
document.getElementById('sopSelect').addEventListener('change', (e) => {
    handleSopSelection(e.target.value);
});

document.getElementById('badSiteSelect').addEventListener('change', (e) => {
    handleBadSiteSelection(e.target.value);
});

document.getElementById('badPointSelect').addEventListener('change', (e) => {
    handleBadPointSelection(e.target.value);
});

document.getElementById('downloadBadPoints').addEventListener('click', downloadBadPointsCSV);

document.getElementById('resetSopBtn').addEventListener('click', () => {
    document.getElementById('sopSelect').value = '';
    document.getElementById('badSiteSelect').value = '';
    document.getElementById('badSiteSelect').disabled = true;
    document.getElementById('badPointSelect').value = '';
    document.getElementById('badPointSelect').disabled = true;
    document.getElementById('downloadBadPoints').disabled = true;
    document.getElementById('badPointsTable').style.display = 'none';
    clearMap();
});

// Initialize on page load
document.addEventListener('DOMContentLoaded', async () => {
    initMap();
    
    // Fetch and display polygon data
    allPolygonsData = await fetchPolygonData();
    
    if (allPolygonsData.estuaries.length > 0 || allPolygonsData.stations.length > 0) {
        addPolygonsToMap(allPolygonsData);
        populateEstuaryDropdown(allPolygonsData);
        showAllPolygons();
    } else {
        alert('No station data available.');
    }
});
