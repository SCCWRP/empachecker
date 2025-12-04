// Define the SOPs - ordered list for table columns
const sopColumns = [
    { name: "Field Grab", code: "sopfield", description: "Field Grab" },
    { name: "SOP 2", code: "sop2", description: "SOP 2: Discrete environmental monitoring - point water quality measurements" },
    { name: "SOP 3a", code: "sop3a", description: "SOP 3: Sediment chemistry" },
    { name: "SOP 3b", code: "sop3b", description: "SOP 3: Sediment toxicity" },
    { name: "SOP 4", code: "sop4", description: "SOP 4: eDNA - field" },
    { name: "SOP 5", code: "sop5", description: "SOP 5: Sediment grain size analysis" },
    { name: "SOP 6a", code: "sop6a", description: "SOP 6: Benthic infauna, small" },
    { name: "SOP 6b", code: "sop6b", description: "SOP 6: Benthic infauna, large" },
    { name: "SOP 7", code: "sop7", description: "SOP 7: Macroalgae" },
    { name: "SOP 8a", code: "sop8a", description: "SOP 8: Fish - BRUVs - Field" },
    { name: "SOP 8b", code: "sop8b", description: "SOP 8: Fish - BRUVs - Lab" },
    { name: "SOP 9", code: "sop9", description: "SOP 9: Fish seines" },
    { name: "SOP 10", code: "sop10", description: "SOP 10: Crab traps" },
    { name: "SOP 11", code: "sop11", description: "SOP 11: Marsh plain vegetation and epifauna surveys" },
    { name: "SOP 13", code: "sop13", description: "SOP 13: Sediment accretion rates" },
    { name: "SOP 15", code: "sop15", description: "SOP 15: Trash monitoring" }
];

// Reverse mapping for modal display
const sopCodeToName = {};
sopColumns.forEach(sop => {
    sopCodeToName[sop.code] = sop.description;
});

let inventoryData = {};
let flatData = []; // Flat array for easier filtering/display

// Function to fetch inventory data from the backend
async function fetchInventoryData() {
    try {
        const response = await fetch('/empachecker/get-inventory-data');
        if (!response.ok) {
            throw new Error('Network response was not ok');
        }
        const data = await response.json();
        return data;
    } catch (error) {
        console.error('Error fetching inventory data:', error);
        return null;
    }
}

// Transform nested data into flat array structure
function transformToFlatData(data) {
    const flat = [];
    const generalData = data.general.data;
    
    // Get all unique site IDs across all SOPs
    const allSites = new Set();
    const allYears = new Set();
    
    Object.keys(generalData).forEach(sopCode => {
        Object.keys(generalData[sopCode]).forEach(siteId => {
            allSites.add(siteId);
            Object.keys(generalData[sopCode][siteId]).forEach(year => {
                allYears.add(year);
            });
        });
    });
    
    // Create flat rows: one row per siteId + year + season combination
    const seasons = ['Spring', 'Fall'];
    const sortedSites = Array.from(allSites).sort();
    const sortedYears = Array.from(allYears).sort();
    
    sortedSites.forEach(siteId => {
        sortedYears.forEach(year => {
            seasons.forEach(season => {
                const row = {
                    siteId,
                    year,
                    season,
                    sops: {}
                };
                
                // Populate each SOP column
                sopColumns.forEach(sop => {
                    const sopData = generalData[sop.code];
                    if (sopData && sopData[siteId] && sopData[siteId][year]) {
                        row.sops[sop.code] = sopData[siteId][year][season] || 'Not Assigned';
                    } else {
                        row.sops[sop.code] = 'Not Assigned';
                    }
                });
                
                flat.push(row);
            });
        });
    });
    
    return flat;
}

// Create table headers
function createTableHeaders() {
    const headerRow = document.getElementById('tableHeader');
    const filterRow = document.getElementById('filterRow');
    headerRow.innerHTML = '';
    filterRow.innerHTML = '';
    
    // Fixed columns
    const fixedHeaders = ['Site ID', 'Year', 'Season'];
    fixedHeaders.forEach((header, index) => {
        const th = document.createElement('th');
        th.innerText = header;
        headerRow.appendChild(th);
        
        // Filter cell
        const filterTh = document.createElement('th');
        filterTh.style.padding = '4px';
        
        if (header === 'Season') {
            // Dropdown for Season filter
            const filterSelect = document.createElement('select');
            filterSelect.className = 'form-select form-select-sm column-filter';
            filterSelect.dataset.column = 'season';
            filterSelect.innerHTML = `
                <option value="">All</option>
                <option value="spring">Spring</option>
                <option value="fall">Fall</option>
            `;
            filterSelect.addEventListener('change', filterAndRenderTable);
            filterTh.appendChild(filterSelect);
        } else {
            // Empty filter cell for Site ID and Year (no filter needed)
            filterTh.innerText = '';
        }
        filterRow.appendChild(filterTh);
    });
    
    // SOP columns
    sopColumns.forEach(sop => {
        const th = document.createElement('th');
        th.style.cursor = 'pointer';
        
        // Create text span
        const textSpan = document.createElement('span');
        textSpan.innerText = sop.name;
        th.appendChild(textSpan);
        
        // Create info icon with Bootstrap tooltip
        const infoIcon = document.createElement('span');
        infoIcon.innerHTML = ' &#9432;'; // Unicode info symbol
        infoIcon.style.fontSize = '0.8em';
        infoIcon.style.opacity = '0.7';
        infoIcon.setAttribute('data-bs-toggle', 'tooltip');
        infoIcon.setAttribute('data-bs-placement', 'top');
        infoIcon.setAttribute('title', sop.description);
        th.appendChild(infoIcon);
        
        // Also show alert on click for mobile/accessibility
        th.addEventListener('click', () => {
            alert(sop.description);
        });
        
        headerRow.appendChild(th);
        
        // Filter cell for SOP
        const filterTh = document.createElement('th');
        filterTh.style.padding = '4px';
        const filterSelect = document.createElement('select');
        filterSelect.className = 'form-select form-select-sm column-filter';
        filterSelect.dataset.column = sop.code;
        filterSelect.innerHTML = `
            <option value="">All</option>
            <option value="Data Available">Data Available</option>
            <option value="Not Submitted">Not Submitted</option>
            <option value="Not Assigned">Not Assigned</option>
        `;
        filterSelect.addEventListener('change', filterAndRenderTable);
        filterTh.appendChild(filterSelect);
        filterRow.appendChild(filterTh);
    });
    
    // Initialize Bootstrap tooltips
    const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]');
    tooltipTriggerList.forEach(el => new bootstrap.Tooltip(el));
}

// Create year radio buttons (single select)
function createYearCheckboxes() {
    const container = document.getElementById('yearCheckboxContainer');
    container.innerHTML = '';
    
    const minYear = parseInt(inventoryData.general.minYear);
    const maxYear = parseInt(inventoryData.general.maxYear);
    const currentYear = new Date().getFullYear();
    
    for (let year = minYear; year <= maxYear; year++) {
        const wrapper = document.createElement('div');
        wrapper.className = 'form-check form-check-inline';
        
        const radio = document.createElement('input');
        radio.type = 'radio';
        radio.className = 'form-check-input year-radio';
        radio.name = 'yearSelect';
        radio.id = `year-${year}`;
        radio.value = year;
        radio.checked = (year === currentYear); // Default to current year only
        radio.addEventListener('change', filterAndRenderTable);
        
        const label = document.createElement('label');
        label.className = 'form-check-label';
        label.htmlFor = `year-${year}`;
        label.innerText = year;
        
        wrapper.appendChild(radio);
        wrapper.appendChild(label);
        container.appendChild(wrapper);
    }
}

// Get selected year from radio buttons
function getSelectedYears() {
    const selectedRadio = document.querySelector('.year-radio:checked');
    return selectedRadio ? [selectedRadio.value] : [];
}

// Get site filter value
function getSiteFilter() {
    const input = document.getElementById('siteFilter');
    return input ? input.value.toLowerCase().trim() : '';
}

// Get column filters
function getColumnFilters() {
    const filters = {};
    document.querySelectorAll('.column-filter').forEach(input => {
        const column = input.dataset.column;
        const value = input.value.toLowerCase().trim();
        if (value) {
            filters[column] = value;
        }
    });
    return filters;
}

// Filter and render the table
function filterAndRenderTable() {
    const selectedYears = getSelectedYears();
    const siteFilter = getSiteFilter();
    const siteFilters = siteFilter ? siteFilter.split(',').map(s => s.trim()).filter(s => s) : [];
    const columnFilters = getColumnFilters();
    
    // Filter flat data
    let filteredData = flatData.filter(row => {
        // Year filter (from radio button)
        if (!selectedYears.includes(row.year)) {
            return false;
        }
        
        // Site filter (from top filter input)
        if (siteFilters.length > 0) {
            const matches = siteFilters.some(filter => 
                row.siteId.toLowerCase().includes(filter)
            );
            if (!matches) return false;
        }
        
        // Column filters
        for (const [column, filterValue] of Object.entries(columnFilters)) {
            if (column === 'season') {
                if (!row.season.toLowerCase().includes(filterValue)) return false;
            } else {
                // SOP column filter
                const sopValue = (row.sops[column] || '').toLowerCase();
                if (!sopValue.includes(filterValue)) return false;
            }
        }
        
        return true;
    });
    
    renderTable(filteredData);
}

// Render the table with given data
function renderTable(data) {
    const tbody = document.getElementById('tableBody');
    tbody.innerHTML = '';
    
    data.forEach(row => {
        const tr = document.createElement('tr');
        
        // Site ID cell
        const siteCell = document.createElement('td');
        siteCell.innerText = row.siteId;
        tr.appendChild(siteCell);
        
        // Year cell
        const yearCell = document.createElement('td');
        yearCell.innerText = row.year;
        tr.appendChild(yearCell);
        
        // Season cell
        const seasonCell = document.createElement('td');
        seasonCell.innerText = row.season;
        tr.appendChild(seasonCell);
        
        // SOP cells
        sopColumns.forEach(sop => {
            const cell = document.createElement('td');
            const cellValue = row.sops[sop.code] || 'Not Assigned';
            cell.innerText = cellValue;
            
            // Add CSS classes based on value
            if (cellValue.includes('Data Available')) {
                cell.classList.add('green-cell');
                cell.style.cursor = 'pointer';
                // Attach click listener to open modal
                cell.addEventListener('click', () => {
                    showCellInfoModal(sop.description, row.siteId, row.year, row.season, cellValue);
                });
            } else if (cellValue.includes('Not Submitted')) {
                cell.classList.add('red-cell');
            }
            
            tr.appendChild(cell);
        });
        
        tbody.appendChild(tr);
    });
}

// When a cell is clicked, show modal with details
function showCellInfoModal(sopName, siteID, year, season, cellValue) {
    // Update the modal with basic information
    document.getElementById('modalSop').innerText = sopName;
    document.getElementById('modalSiteId').innerText = siteID;
    document.getElementById('modalSeason').innerText = season;
    document.getElementById('modalYear').innerText = year;
    
    // Call to Flask route to fetch additional data
    fetch(`/empachecker/get-sample-data?sop=${encodeURIComponent(sopName)}&siteid=${encodeURIComponent(siteID)}&year=${encodeURIComponent(year)}&season=${encodeURIComponent(season)}`)
        .then(response => response.json())
        .then(data => {
            // Populate the modal with additional data from Flask
            document.getElementById('modalSampleCollectionDate').innerText = data.samplecollectiondate || 'N/A';
            document.getElementById('modalCreatedDate').innerText = data.created_date || 'N/A';
        })
        .catch(error => {
            console.error('Error fetching sample data:', error);
            document.getElementById('modalSampleCollectionDate').innerText = 'Error fetching data';
            document.getElementById('modalCreatedDate').innerText = 'Error fetching data';
        });

    // Show the modal
    const modal = new bootstrap.Modal(document.getElementById('infoModal'));
    modal.show();
}

// Show loader
function showLoader() {
    const loader = document.getElementById('loader');
    if (loader) {
        loader.style.display = 'block';
    }
}

// Hide loader
function hideLoader() {
    const loader = document.getElementById('loader');
    if (loader) {
        loader.style.display = 'none';
    }
}

// Modal for year selection before download
function showDownloadYearModal() {
    let modal = document.getElementById('downloadYearModal');
    const maxYear = inventoryData.general ? inventoryData.general.maxYear : new Date().getFullYear();
    const minYear = inventoryData.general ? inventoryData.general.minYear : 2021;
    
    if (!modal) {
        modal = document.createElement('div');
        modal.id = 'downloadYearModal';
        modal.className = 'modal fade';
        modal.tabIndex = -1;
        modal.setAttribute('aria-hidden', 'true');
        
        // Build year options dynamically
        let yearOptions = '';
        for (let y = parseInt(maxYear); y >= parseInt(minYear); y--) {
            yearOptions += `<option value="${y}"${y == maxYear ? ' selected' : ''}>${y}</option>`;
        }
        
        modal.innerHTML = `
        <div class="modal-dialog">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title">Select Year to Download</h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                </div>
                <div class="modal-body">
                    <form id="downloadYearForm">
                        <div class="mb-3">
                            <label for="downloadYearSelect" class="form-label">Year</label>
                            <select class="form-select" id="downloadYearSelect" required>
                                ${yearOptions}
                            </select>
                        </div>
                    </form>
                </div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
                    <button type="button" class="btn btn-primary" id="confirmDownloadYear">Download</button>
                </div>
            </div>
        </div>
        `;
        document.body.appendChild(modal);
    } else {
        // Update selection to most recent year
        document.getElementById('downloadYearSelect').value = maxYear;
    }

    // Add event listener for download button
    document.getElementById('confirmDownloadYear').onclick = function() {
        const year = document.getElementById('downloadYearSelect').value;
        downloadInventoryData(year);
        const bsModal = bootstrap.Modal.getOrCreateInstance(modal);
        bsModal.hide();
    };

    const bsModal = new bootstrap.Modal(modal);
    bsModal.show();
}

// Download inventory data
function downloadInventoryData(year) {
    if (!year) {
        showDownloadYearModal();
        return;
    }
    window.open(`/empachecker/download-inventory-data?year=${year}`, '_blank');
}

// Initialize page
document.addEventListener('DOMContentLoaded', async () => {
    showLoader();

    const data = await fetchInventoryData();

    if (data) {
        // Store the data
        inventoryData = data;
        
        // Transform to flat structure
        flatData = transformToFlatData(data);
        
        // Create table headers
        createTableHeaders();
        
        // Create year checkboxes
        createYearCheckboxes();
        
        // Initial render
        filterAndRenderTable();
        
        // Set up event listeners
        document.getElementById('siteFilter').addEventListener('input', filterAndRenderTable);
        document.getElementById('downloadBtn').addEventListener('click', () => downloadInventoryData());
    }
    
    hideLoader();
});
