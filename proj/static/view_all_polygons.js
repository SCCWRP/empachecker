// Global variables
let map;
let estuaryLayers = [];
let stationLayers = [];
let allPolygonsData = {};
let estuaryGroups = {};

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
