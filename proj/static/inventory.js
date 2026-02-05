// Define the SOPs - ordered list for table columns (SOP 2-15 only)
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

// SOP 1 data
let sop1Data = [];
let sop1FilteredData = [];

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

// Transform nested data into flat array structure (SOP 2-15 only)
function transformToFlatData(data) {
    const flat = [];
    const generalData = data.general.data;

    // Get all unique site IDs across all SOPs
    const allSites = new Set();
    const allYears = new Set();

    Object.keys(generalData).forEach(sopCode => {
        // Skip SOP 1 - it's in a separate table now
        if (sopCode === 'sop1') return;

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
        radio.checked = (year === 2025); // Default to 2025
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

    // Check if this is SOP 1 (logger data)
    const isSop1 = sopName.includes('SOP 1');

    if (isSop1) {
        // Hide the Sample Collection Date and Submitted Date rows for SOP 1
        document.getElementById('modalSampleCollectionDateRow').style.display = 'none';
        document.getElementById('modalCreatedDateRow').style.display = 'none';

        // For SOP 1, fetch and display raw_ column details
        fetch(`/empachecker/get-sop1-details?siteid=${encodeURIComponent(siteID)}&year=${encodeURIComponent(year)}&season=${encodeURIComponent(season)}`)
            .then(response => response.json())
            .then(data => {
                if (data.raw_data) {
                    // Build a table to display raw_ columns
                    let tableHtml = '<div style="margin-top: 15px;"><strong>Raw Data Parameters:</strong><br><table class="table table-sm table-bordered" style="margin-top: 10px;"><thead><tr><th>Parameter</th><th>Data Available</th></tr></thead><tbody>';

                    for (const [param, value] of Object.entries(data.raw_data)) {
                        const displayValue = value === 'y' ? '✓ Yes' : '✗ No';
                        const rowClass = value === 'y' ? 'table-success' : '';
                        tableHtml += `<tr class="${rowClass}"><td>${param}</td><td>${displayValue}</td></tr>`;
                    }

                    tableHtml += '</tbody></table></div>';

                    // Add the raw data table to the modal body
                    const modalBody = document.querySelector('#infoModal .modal-body');
                    // Remove any existing raw data table
                    const existingTable = modalBody.querySelector('.sop1-raw-data');
                    if (existingTable) {
                        existingTable.remove();
                    }
                    // Add the new table
                    const tableDiv = document.createElement('div');
                    tableDiv.className = 'sop1-raw-data';
                    tableDiv.innerHTML = tableHtml;
                    modalBody.appendChild(tableDiv);
                } else {
                    // Show error in modal body
                    const modalBody = document.querySelector('#infoModal .modal-body');
                    const existingTable = modalBody.querySelector('.sop1-raw-data');
                    if (existingTable) {
                        existingTable.remove();
                    }
                    const errorDiv = document.createElement('div');
                    errorDiv.className = 'sop1-raw-data alert alert-warning';
                    errorDiv.innerText = 'No data found';
                    modalBody.appendChild(errorDiv);
                }
            })
            .catch(error => {
                console.error('Error fetching SOP 1 details:', error);
                const modalBody = document.querySelector('#infoModal .modal-body');
                const existingTable = modalBody.querySelector('.sop1-raw-data');
                if (existingTable) {
                    existingTable.remove();
                }
                const errorDiv = document.createElement('div');
                errorDiv.className = 'sop1-raw-data alert alert-danger';
                errorDiv.innerText = 'Error fetching data';
                modalBody.appendChild(errorDiv);
            });
    } else {
        // Show the Sample Collection Date and Submitted Date rows for other SOPs
        document.getElementById('modalSampleCollectionDateRow').style.display = 'block';
        document.getElementById('modalCreatedDateRow').style.display = 'block';

        // Remove any SOP 1 raw data table if it exists
        const existingTable = document.querySelector('#infoModal .modal-body .sop1-raw-data');
        if (existingTable) {
            existingTable.remove();
        }

        // For other SOPs, fetch sample data as before
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
    }

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

// ============================================
// SOP 1 Table Functions
// ============================================

// Fetch SOP 1 data from backend
async function fetchSop1Data() {
    try {
        const response = await fetch('/empachecker/get-sop1-table-data');
        if (!response.ok) {
            throw new Error('Network response was not ok');
        }
        const data = await response.json();
        return data;
    } catch (error) {
        console.error('Error fetching SOP 1 data:', error);
        return null;
    }
}

// Create SOP 1 table headers
function createSop1TableHeaders() {
    const headerRow = document.getElementById('sop1TableHeader');
    const filterRow = document.getElementById('sop1FilterRow');
    headerRow.innerHTML = '';
    filterRow.innerHTML = '';

    const headers = [
        'Region', 'Site ID', 'Year', 'Month',
        'Chlorophyll', 'Conductivity', 'Depth', 'DO', 'DO %',
        'Temperature', 'ORP', 'pH', 'Pressure', 'Q-Value', 'Salinity', 'Turbidity'
    ];

    // Map headers to column names in data
    const headerToColumn = {
        'Region': 'region',
        'Site ID': 'siteid',
        'Year': 'year',
        'Month': 'month',
        'Chlorophyll': 'raw_chlorophyll',
        'Conductivity': 'raw_conductivity',
        'Depth': 'raw_depth',
        'DO': 'raw_do',
        'DO %': 'raw_do_pct',
        'Temperature': 'raw_h2otemp',
        'ORP': 'raw_orp',
        'pH': 'raw_ph',
        'Pressure': 'raw_pressure',
        'Q-Value': 'raw_qvalue',
        'Salinity': 'raw_salinity',
        'Turbidity': 'raw_turbidity'
    };

    headers.forEach((header, index) => {
        const th = document.createElement('th');
        th.innerText = header;
        headerRow.appendChild(th);

        // Filter row - add inputs for all columns
        const filterTh = document.createElement('th');
        filterTh.style.padding = '4px';

        const filterInput = document.createElement('input');
        filterInput.type = 'text';
        filterInput.className = 'form-control form-control-sm sop1-column-filter';
        filterInput.dataset.column = headerToColumn[header];
        filterInput.placeholder = 'Filter...';
        filterInput.addEventListener('input', filterAndRenderSop1Table);
        filterTh.appendChild(filterInput);

        filterRow.appendChild(filterTh);
    });
}

// Create year radio buttons for SOP 1
function createSop1YearCheckboxes(years) {
    const container = document.getElementById('sop1YearCheckboxContainer');
    container.innerHTML = '';

    years.forEach(year => {
        const wrapper = document.createElement('div');
        wrapper.className = 'form-check form-check-inline';

        const radio = document.createElement('input');
        radio.type = 'radio';
        radio.className = 'form-check-input sop1-year-radio';
        radio.name = 'sop1YearSelect';
        radio.id = `sop1-year-${year}`;
        radio.value = year;
        radio.checked = (year === 2025); // Default to 2025
        radio.addEventListener('change', filterAndRenderSop1Table);

        const label = document.createElement('label');
        label.className = 'form-check-label';
        label.htmlFor = `sop1-year-${year}`;
        label.innerText = year;

        wrapper.appendChild(radio);
        wrapper.appendChild(label);
        container.appendChild(wrapper);
    });
}

// Get selected year for SOP 1
function getSop1SelectedYear() {
    const selectedRadio = document.querySelector('.sop1-year-radio:checked');
    return selectedRadio ? selectedRadio.value : null;
}

// Get SOP 1 site filter
function getSop1SiteFilter() {
    const input = document.getElementById('sop1SiteFilter');
    return input ? input.value.toLowerCase().trim() : '';
}

// Get SOP 1 column filters
function getSop1ColumnFilters() {
    const filters = {};
    document.querySelectorAll('.sop1-column-filter').forEach(input => {
        const column = input.dataset.column;
        const value = input.value.toLowerCase().trim();
        if (value) {
            filters[column] = value;
        }
    });
    return filters;
}

// Filter and render SOP 1 table
function filterAndRenderSop1Table() {
    const siteFilter = getSop1SiteFilter();
    const siteFilters = siteFilter ? siteFilter.split(',').map(s => s.trim()).filter(s => s) : [];
    const columnFilters = getSop1ColumnFilters();

    // Filter data
    let filteredData = sop1Data.filter(row => {
        // Site filter (from top input)
        if (siteFilters.length > 0) {
            const matches = siteFilters.some(filter =>
                row.siteid.toLowerCase().includes(filter)
            );
            if (!matches) return false;
        }

        // Column filters (from table header row)
        for (const [column, filterValue] of Object.entries(columnFilters)) {
            let rowValue = '';

            // Get the value from the row based on column name
            if (row[column] !== null && row[column] !== undefined) {
                rowValue = row[column].toString().toLowerCase();
            } else {
                // For null/undefined values, use 'n' for filtering
                rowValue = 'n';
            }

            if (!rowValue.includes(filterValue)) return false;
        }

        return true;
    });

    sop1FilteredData = filteredData;
    renderSop1Table(filteredData);
}

// Render SOP 1 table
function renderSop1Table(data) {
    const tbody = document.getElementById('sop1TableBody');
    tbody.innerHTML = '';

    data.forEach(row => {
        const tr = document.createElement('tr');

        // Create cells in order
        const columns = [
            'region', 'siteid', 'year', 'month',
            'raw_chlorophyll', 'raw_conductivity', 'raw_depth',
            'raw_do', 'raw_do_pct', 'raw_h2otemp', 'raw_orp',
            'raw_ph', 'raw_pressure', 'raw_qvalue', 'raw_salinity', 'raw_turbidity'
        ];

        const rawColumns = [
            'raw_chlorophyll', 'raw_conductivity', 'raw_depth',
            'raw_do', 'raw_do_pct', 'raw_h2otemp', 'raw_orp',
            'raw_ph', 'raw_pressure', 'raw_qvalue', 'raw_salinity', 'raw_turbidity'
        ];

        columns.forEach(col => {
            const cell = document.createElement('td');

            // For raw_ columns, display 'n' for null/empty values
            if (rawColumns.includes(col)) {
                const cellValue = (row[col] !== null && row[col] !== undefined && row[col] !== '') ? row[col] : 'n';
                cell.innerText = cellValue;

                // Add green color for 'y' values
                if (cellValue === 'y') {
                    cell.classList.add('green-cell');
                }
            } else {
                // For other columns, display the value or empty string
                cell.innerText = row[col] !== null ? row[col] : '';
            }

            tr.appendChild(cell);
        });

        tbody.appendChild(tr);
    });
}

// Initialize page
document.addEventListener('DOMContentLoaded', async () => {
    showLoader();

    // Fetch both SOP 1 and SOP 2-15 data in parallel
    const [inventoryResponse, sop1Response] = await Promise.all([
        fetchInventoryData(),
        fetchSop1Data()
    ]);

    // Initialize SOP 2-15 table
    if (inventoryResponse) {
        // Store the data
        inventoryData = inventoryResponse;

        // Transform to flat structure
        flatData = transformToFlatData(inventoryResponse);

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

    // Initialize SOP 1 table
    if (sop1Response && sop1Response.data) {
        sop1Data = sop1Response.data;

        // Create SOP 1 table headers
        createSop1TableHeaders();

        // Initial render (display all years)
        filterAndRenderSop1Table();

        // Set up event listeners
        document.getElementById('sop1SiteFilter').addEventListener('input', filterAndRenderSop1Table);
    }

    hideLoader();
});
