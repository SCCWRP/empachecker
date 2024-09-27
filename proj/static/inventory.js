// Define the SOPs and Logger parameters
const sopNameMapping = {
    "SOP 2: Discrete environmental monitoring - point water quality measurements": "sop2",
    "SOP 3: Sediment chemistry": "sop3a",
    "SOP 3: Sediment toxicity": "sop3b",
    "SOP 4: eDNA - field": "sop4",
    "SOP 5: Sediment grain size analysis": "sop5",
    "SOP 6: Benthic infauna, small": "sop6a",
    "SOP 6: Benthic infauna, large": "sop6b",
    "SOP 7: Macroalgae": "sop7",
    "SOP 8: Fish - BRUVs - Field": "sop8",
    "SOP 8: Fish - BRUVs - Lab": "sop8b",
    "SOP 9: Fish seines": "sop9",
    "SOP 10: Crab traps": "sop10",
    "SOP 11: Marsh plain vegetation and epifauna surveys": "sop11",
    "SOP 13: Sediment accretion rates": "sop13",
    "SOP 15: Trash monitoring": "sop15"
};

const loggerParameters = [
    'raw_chlorophyll', 'raw_conductivity', 'raw_depth', 'raw_do', 'raw_do_pct',
    'raw_h2otemp', 'raw_orp', 'raw_ph', 'raw_pressure', 'raw_qvalue',
    'raw_salinity', 'raw_turbidity'
];

let inventoryData = {};

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

// Function to create sub-tabs and content for both General and Logger
function createTabsAndContent(tabType, items) {
    const subTabsContainer = document.getElementById(`${tabType}SubTabs`);
    const tabContentContainer = document.getElementById(`${tabType}TabContent`);

    items.forEach((item, index) => {
        // Determine tab name and ID based on whether it's general or logger
        let tabName, tabID;
        if (tabType === 'general') {
            // If general, item represents the descriptive SOP name
            tabName = item;
            tabID = sopNameMapping[item]; // Fetch the corresponding SOP code from the mapping
        } else {
            // If logger, item represents the parameter directly
            tabName = item;
            tabID = item;
        }

        // Create the tab header
        const tabHeader = document.createElement('li');
        tabHeader.className = 'nav-item';
        tabHeader.setAttribute('role', 'presentation');

        const tabButton = document.createElement('button');
        tabButton.className = `nav-link ${index === 0 ? 'active' : ''}`;
        tabButton.id = `${tabID}-tab`;
        tabButton.setAttribute('data-bs-toggle', 'tab');
        tabButton.setAttribute('data-bs-target', `#${tabID}`);
        tabButton.setAttribute('type', 'button');
        tabButton.setAttribute('role', 'tab');
        tabButton.setAttribute('aria-controls', tabID);
        tabButton.setAttribute('aria-selected', index === 0 ? 'true' : 'false');
        tabButton.innerText = tabName;

        tabHeader.appendChild(tabButton);
        subTabsContainer.appendChild(tabHeader);

        // Create the tab content container
        const tabContent = document.createElement('div');
        tabContent.className = `tab-pane fade ${index === 0 ? 'show active' : ''}`;
        tabContent.id = tabID;
        tabContent.setAttribute('role', 'tabpanel');
        tabContent.setAttribute('aria-labelledby', `${tabID}-tab`);

        // Create a filter input field for Site ID filtering
        const filterContainer = document.createElement('div');
        filterContainer.className = 'mb-3';
        const filterInput = document.createElement('input');
        filterInput.type = 'text';
        filterInput.placeholder = 'Filter by Site ID. You can enter multiple IDs separated by commas.';
        filterInput.className = 'form-control site-filter';
        filterInput.dataset.tableId = `table_${tabID}`; // Associate the filter with the specific table
        filterContainer.appendChild(filterInput);

        // Create the legend container
        const legendContainer = document.createElement('div');
        legendContainer.className = 'mb-3 d-flex align-items-center'; // Use flexbox for horizontal alignment
        legendContainer.style.gap = '10px'; // Add some spacing between legend items

        // Add the legend items
        const legendTexts = [
            { text: "Info:", class: 'text-muted' },
            { text: "Data Available: Can be downloaded using the Advanced Query Tool on empa.sccwrp.org", class: 'text-success' },
            { text: "Not Submitted: Data are expected for this site, but not submitted yet", class: 'text-danger' },
            { text: "Not Assigned: Data are not expected for this site", class: 'text-muted' }
        ];

        legendTexts.forEach(legend => {
            const legendItem = document.createElement('span'); // Use span instead of p for inline display
            legendItem.innerText = legend.text;
            legendItem.className = legend.class;
            legendContainer.appendChild(legendItem);
        });

        // Create a checkbox container for year selection
        const yearCheckboxContainer = document.createElement('div');
        yearCheckboxContainer.className = 'mb-3';

        const minYear = tabType === 'general' ? inventoryData.general.minYear : inventoryData.logger.minYear;
        const maxYear = tabType === 'general' ? inventoryData.general.maxYear : inventoryData.logger.maxYear;

        const label = document.createElement('label');
        label.innerText = 'Toggle Years: ';
        yearCheckboxContainer.appendChild(label);

        for (let year = minYear; year <= maxYear; year++) {
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.checked = true; // Initially checked (visible)
            checkbox.className = 'year-checkbox';
            checkbox.dataset.tableId = `table_${tabID}`;
            checkbox.dataset.year = year;
            checkbox.style.marginLeft = '5px';

            const checkboxLabel = document.createElement('span');
            checkboxLabel.innerText = ` ${year} `;

            yearCheckboxContainer.appendChild(checkbox);
            yearCheckboxContainer.appendChild(checkboxLabel);
        }

        // Add "Download Inventory Data" and "Refresh Inventory" buttons
        if (tabType === 'general') {
            const buttonContainer = document.createElement('div');
            buttonContainer.className = 'mb-3 d-flex align-items-center';
            buttonContainer.style.gap = '10px';

            const downloadButton = document.createElement('button');
            downloadButton.innerText = 'Download Inventory Data';
            downloadButton.className = 'btn btn-primary';
            downloadButton.onclick = () => downloadInventoryData(); 

            const refreshButton = document.createElement('button');
            refreshButton.innerText = 'Refresh Inventory';
            refreshButton.className = 'btn btn-secondary';
            refreshButton.onclick = () => refreshInventory();

            buttonContainer.appendChild(downloadButton);
            //buttonContainer.appendChild(refreshButton);

            yearCheckboxContainer.appendChild(buttonContainer);
        } else {
            const buttonContainer = document.createElement('div');
            buttonContainer.className = 'mb-3 d-flex align-items-center';
            buttonContainer.style.gap = '10px';

            const downloadButton = document.createElement('button');
            downloadButton.innerText = 'Download Inventory Logger Data';
            downloadButton.className = 'btn btn-primary';
            downloadButton.onclick = () => downloadInventoryLoggerData();

            buttonContainer.appendChild(downloadButton);
            yearCheckboxContainer.appendChild(buttonContainer);
        }

        // Create the table inside the tab content
        const tableContainer = document.createElement('div');
        tableContainer.className = 'table-responsive';

        const table = document.createElement('table');
        table.className = 'table table-bordered w-100 text-center';
        table.id = `table_${tabID}`;

        const thead = document.createElement('thead');
        thead.className = 'table-primary';
        table.appendChild(thead);

        const tbody = document.createElement('tbody');
        table.appendChild(tbody);

        tableContainer.appendChild(table);

        // Append all elements to the tab content
        tabContent.appendChild(filterContainer); // Add the filter input above the table
        tabContent.appendChild(legendContainer); // Add the legend container below the filter input
        tabContent.appendChild(yearCheckboxContainer); // Add the checkbox container above the table
        tabContent.appendChild(tableContainer);
        tabContentContainer.appendChild(tabContent);
    });
}


// Function to create table headers for a given table
function createTableHeaders(tableHead, minYear, maxYear, tabType) {
    tableHead.innerHTML = ''; // Clear existing headers
    const headerRow1 = document.createElement('tr');

    const siteIDTh = document.createElement('th');
    siteIDTh.innerText = 'Site ID';
    headerRow1.appendChild(siteIDTh);

    for (let year = minYear; year <= maxYear; year++) {
        const yearHeader = document.createElement('th');
        yearHeader.setAttribute('colspan', tabType === 'general' ? 2 : 12); // Use 2 columns for general, 12 for logger
        yearHeader.innerText = year;
        headerRow1.appendChild(yearHeader);
    }

    tableHead.appendChild(headerRow1);

    const headerRow2 = document.createElement('tr');
    const emptyTh = document.createElement('th'); // Placeholder for "Site ID"
    headerRow2.appendChild(emptyTh);

    for (let year = minYear; year <= maxYear; year++) {
        if (tabType === 'general') {
            // Add "Spring" and "Fall" columns
            const springHeader = document.createElement('th');
            springHeader.innerText = 'Spring';
            headerRow2.appendChild(springHeader);

            const fallHeader = document.createElement('th');
            fallHeader.innerText = 'Fall';
            headerRow2.appendChild(fallHeader);
        } else {
            // For logger, add the 12 months
            for (let month = 1; month <= 12; month++) {
                const monthHeader = document.createElement('th');
                monthHeader.innerText = month;
                headerRow2.appendChild(monthHeader);
            }
        }
    }

    tableHead.appendChild(headerRow2);
}

// Function to populate the table body with data
function populateTableBody(type, parameter, tableBody) {
    tableBody.innerHTML = ''; // Clear existing body rows

    const dataForParameter = inventoryData[type].data[parameter] || {}; // Fetch data for the current parameter/SOP
    const sites = Object.keys(dataForParameter);

    sites.forEach(siteID => {
        const row = document.createElement('tr');

        const siteIDCell = document.createElement('td');
        siteIDCell.innerText = siteID;
        row.appendChild(siteIDCell);

        for (let year = inventoryData[type].minYear; year <= inventoryData[type].maxYear; year++) {
            const yearData = dataForParameter[siteID]?.[year.toString()] || {};

            if (type === 'general') {
                // Handle Spring and Fall for general tab
                const springCell = document.createElement('td');
                const springValue = yearData['Spring'] || 'Not Submitted'; // Default to 'Not Submitted' if not found
                springCell.innerText = springValue;
                if (springValue === 'Data Available') {
                    springCell.classList.add('green-cell');
                } else if (springValue === 'Not Submitted') {
                    springCell.classList.add('red-cell');
                }
                row.appendChild(springCell);

                const fallCell = document.createElement('td');
                const fallValue = yearData['Fall'] || 'Not Submitted'; // Default to 'Not Submitted' if not found
                fallCell.innerText = fallValue;
                if (fallValue === 'Data Available') {
                    fallCell.classList.add('green-cell');
                } else if (fallValue === 'Not Submitted') {
                    fallCell.classList.add('red-cell');
                }
                
                row.appendChild(fallCell);
            } else {
                // For logger, iterate through 12 months
                for (let month = 1; month <= 12; month++) {
                    const cell = document.createElement('td');
                    const cellValue = yearData[month.toString()] || 'n'; // Default to 'n' if not found

                    cell.innerText = cellValue;

                    // Add the green-cell class if the cell value is 'y'
                    if (cellValue === 'y') {
                        cell.classList.add('green-cell');
                    } 

                    row.appendChild(cell);
                }
            }
        }

        tableBody.appendChild(row);
    });
}

// Event listener to handle clicks on sub-tabs
function setupTabListeners(tabType, items) {
    const subTabsContainer = document.getElementById(`${tabType}SubTabs`);

    // Iterate over the items list
    items.forEach((item) => {
        let tabID = tabType === 'general' ? sopNameMapping[item] : item;

        // Find the tab button by ID and set up the click listener
        const tabButton = document.getElementById(`${tabID}-tab`);
        
        tabButton.addEventListener('click', () => {
            const tableID = `table_${tabID}`;
            const tableHead = document.getElementById(tableID).getElementsByTagName('thead')[0];
            const tableBody = document.getElementById(tableID).getElementsByTagName('tbody')[0];

            // Create table headers and populate body for the selected parameter/SOP
            createTableHeaders(tableHead, inventoryData[tabType].minYear, inventoryData[tabType].maxYear, tabType);
            populateTableBody(tabType, tabID, tableBody);
        });
    });
}



// Function to filter table rows based on multiple siteIDs separated by commas
function filterTableBySiteID(event) {
    const filterValue = event.target.value.toLowerCase(); // Get the filter input and convert to lowercase
    const filterValuesArray = filterValue.split(',').map(value => value.trim()); // Split by commas and trim each value
    const tableID = event.target.dataset.tableId; // Get the associated table ID
    const table = document.getElementById(tableID);
    const rows = table.getElementsByTagName('tbody')[0].getElementsByTagName('tr');

    for (const row of rows) {
        const siteIDCell = row.getElementsByTagName('td')[0]; // Assuming the first cell contains the siteID
        if (siteIDCell) {
            const siteIDText = siteIDCell.textContent || siteIDCell.innerText;
            // Check if any of the filter values match the siteID
            const isMatch = filterValuesArray.some(value => siteIDText.toLowerCase().includes(value));
            row.style.display = isMatch ? '' : 'none';
        }
    }
}


// Function to hide/unhide columns based on year selection
function toggleYearColumns(event) {
    const year = parseInt(event.target.dataset.year); // Get the year from the checkbox
    const tableID = event.target.dataset.tableId; // Get the associated table ID
    const table = document.getElementById(tableID);

    // Determine whether we're in the general or logger tab
    const tabType = tableID.includes("sop") ? 'general' : 'logger';

    // Retrieve the correct minYear and maxYear based on the tab type
    const minYear = inventoryData[tabType].minYear; 
    const maxYear = inventoryData[tabType].maxYear;

    // Adjust the number of columns per year based on the tab type
    const columnsPerYear = tabType === 'general' ? 2 : 12; // 2 columns for general (Spring, Fall), 12 for logger

    // Calculate the column index range for the selected year
    const yearIndex = year - minYear; // Calculate the relative index of the year
    const startIndex = 1 + (yearIndex * columnsPerYear); // Start column index (1-based due to 'Site ID')
    const endIndex = startIndex + columnsPerYear - 1; // End column index

    // Iterate through each row
    const rows = table.getElementsByTagName('tr');
    
    for (const row of rows) {
        const cells = Array.from(row.children); // Get all cells in the row as an array

        // Calculate the actual start and end indices for the row
        let currentColIndex = 0; // Track the current column index within the row
        
        for (let i = 0; i < cells.length; i++) {
            const cell = cells[i];
            const colSpan = cell.colSpan || 1; // Get the column span of the cell (default to 1)

            // Check if the current cell range falls within the year range we're toggling
            if (currentColIndex >= startIndex && currentColIndex <= endIndex) {
                cell.style.display = event.target.checked ? '' : 'none'; // Show or hide the cell
            }

            // Update the current column index by adding the column span
            currentColIndex += colSpan;

            // Stop the loop if we've exceeded the endIndex
            if (currentColIndex > endIndex) break;
        }
    }
}


function showLoader() {
    const loader = document.getElementById('loader');
    if (loader) {
        loader.style.display = 'block';
    }
}

// Function to hide the loader
function hideLoader() {
    const loader = document.getElementById('loader');
    if (loader) {
        loader.style.display = 'none';
    }
}

// Function to download the original general_df
function downloadInventoryData() {
    window.open('/empachecker/download-inventory-data', '_blank');
}

function downloadInventoryLoggerData() {
    window.open('/empachecker/download-inventory-logger-data', '_blank');
}

// Function to refresh inventory data
async function refreshInventory() {
    alert('Sending request to refresh inventory data. This may take up to 5 minutes. Please wait until the loader disappears.');

    showLoader(); // Show loader while refreshing data

    try {
        const response = await fetch('/empachecker/refresh-inventory', { method: 'POST' });
        if (!response.ok) {
            throw new Error('Failed to refresh inventory');
        }

        // After refreshing, reload the inventory data
        const data = await fetchInventoryData();
        if (data) {
            // Update the inventory data
            Object.assign(inventoryData, data);
            // You may want to reinitialize your tabs or refresh the display
            alert('Inventory refreshed successfully. Refresh the page to see the most up-to-date data.');
        }
    } catch (error) {
        console.error('Error refreshing inventory:', error);
        alert('Failed to refresh inventory');
    } finally {
        hideLoader(); // Hide loader after operation completes
    }
}




// Attach the toggleYearColumns function to all year checkboxes
document.addEventListener('change', (event) => {
    if (event.target.classList.contains('year-checkbox')) {
        toggleYearColumns(event);
    }
});



// Attach the filterTableBySiteID function to all filter inputs
document.addEventListener('input', (event) => {
    if (event.target.classList.contains('site-filter')) {
        filterTableBySiteID(event);
    }
});


// Fetch and populate the inventoryData when the page loads
document.addEventListener('DOMContentLoaded', async () => {
    showLoader(); // Show loader while fetching data

    const data = await fetchInventoryData();

    if (data) {
        // Populate the inventoryData object
        Object.assign(inventoryData, data);

        // Create tabs and content
        createTabsAndContent('general', Object.keys(sopNameMapping));
        createTabsAndContent('logger', loggerParameters);

        // Set up tab listeners
        setupTabListeners('general', Object.keys(sopNameMapping));
        setupTabListeners('logger', loggerParameters);

        // Automatically trigger the first tab's click event to populate it on page load
        document.getElementById('sop2-tab').click();
        document.getElementById('raw_chlorophyll-tab').click();
    }
    
    hideLoader(); 

});

