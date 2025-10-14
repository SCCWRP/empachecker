// Global variables
let map;
let polygonLayers = [];
let allPolygonsData = [];
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
        return [];
    } finally {
        hideLoader();
    }
}

// Generate a color for each estuary
function getEstuaryColor(estuaryName, index) {
    const colors = [
        '#3388ff', '#ff3333', '#33ff33', '#ffff33', '#ff33ff',
        '#33ffff', '#ff8833', '#8833ff', '#33ff88', '#ff3388'
    ];
    return colors[index % colors.length];
}

// Add polygons to the map
function addPolygonsToMap(polygonsData) {
    // Clear existing layers
    polygonLayers.forEach(layer => map.removeLayer(layer));
    polygonLayers = [];
    estuaryGroups = {};

    // Group by estuary
    const estuariesMap = {};
    polygonsData.forEach(station => {
        if (!estuariesMap[station.estuaryname]) {
            estuariesMap[station.estuaryname] = [];
        }
        estuariesMap[station.estuaryname].push(station);
    });

    // Create layers for each polygon
    let colorIndex = 0;
    Object.keys(estuariesMap).forEach(estuaryName => {
        const color = getEstuaryColor(estuaryName, colorIndex++);
        const stations = estuariesMap[estuaryName];
        
        estuaryGroups[estuaryName] = [];

        stations.forEach(station => {
            try {
                const geojson = JSON.parse(station.geometry);
                
                const layer = L.geoJSON(geojson, {
                    style: {
                        color: color,
                        weight: 2,
                        opacity: 0.8,
                        fillOpacity: 0.3
                    }
                }).bindPopup(`
                    <strong>Estuary:</strong> ${station.estuaryname}<br>
                    <strong>Site ID:</strong> ${station.siteid}<br>
                    <strong>Station No:</strong> ${station.stationno}
                `);

                layer.addTo(map);
                polygonLayers.push(layer);
                estuaryGroups[estuaryName].push(layer);
            } catch (e) {
                console.error(`Error parsing geometry for ${station.siteid}:`, e);
            }
        });
    });
}

// Populate estuary dropdown
function populateEstuaryDropdown(polygonsData) {
    const estuarySelect = document.getElementById('estuarySelect');
    const estuaries = [...new Set(polygonsData.map(p => p.estuaryname))].sort();

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
    polygonLayers.forEach(layer => {
        map.removeLayer(layer);
    });

    // Show only selected estuary polygons
    const selectedLayers = estuaryGroups[estuaryName] || [];
    if (selectedLayers.length > 0) {
        const bounds = L.latLngBounds([]);
        selectedLayers.forEach(layer => {
            layer.addTo(map);
            bounds.extend(layer.getBounds());
        });
        
        // Zoom to fit all polygons of selected estuary
        map.fitBounds(bounds, { padding: [50, 50] });
    }
}

// Show all polygons
function showAllPolygons() {
    polygonLayers.forEach(layer => {
        if (!map.hasLayer(layer)) {
            layer.addTo(map);
        }
    });

    // Fit bounds to all polygons
    if (polygonLayers.length > 0) {
        const bounds = L.latLngBounds([]);
        polygonLayers.forEach(layer => {
            bounds.extend(layer.getBounds());
        });
        map.fitBounds(bounds, { padding: [50, 50] });
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
    
    if (allPolygonsData.length > 0) {
        addPolygonsToMap(allPolygonsData);
        populateEstuaryDropdown(allPolygonsData);
        showAllPolygons();
    } else {
        alert('No station data available.');
    }
});
