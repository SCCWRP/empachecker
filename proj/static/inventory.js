// Define the SOPs and Logger parameters
const generalSOPs = ["sop2", "sop4", "sop6", "sop7", "sop8", "sop9", "sop10", "sop11", "sop13"];
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
        // Create the tab header
        const tabHeader = document.createElement('li');
        tabHeader.className = 'nav-item';
        tabHeader.setAttribute('role', 'presentation');

        const tabButton = document.createElement('button');
        tabButton.className = `nav-link ${index === 0 ? 'active' : ''}`;
        tabButton.id = `${item}-tab`;
        tabButton.setAttribute('data-bs-toggle', 'tab');
        tabButton.setAttribute('data-bs-target', `#${item}`);
        tabButton.setAttribute('type', 'button');
        tabButton.setAttribute('role', 'tab');
        tabButton.setAttribute('aria-controls', item);
        tabButton.setAttribute('aria-selected', index === 0 ? 'true' : 'false');
        tabButton.innerText = tabType === 'general' ? item : item;

        tabHeader.appendChild(tabButton);
        subTabsContainer.appendChild(tabHeader);

        // Create the tab content container
        const tabContent = document.createElement('div');
        tabContent.className = `tab-pane fade ${index === 0 ? 'show active' : ''}`;
        tabContent.id = item;
        tabContent.setAttribute('role', 'tabpanel');
        tabContent.setAttribute('aria-labelledby', `${item}-tab`);

        // Create a filter input field for Site ID filtering
        const filterContainer = document.createElement('div');
        filterContainer.className = 'mb-3';
        const filterInput = document.createElement('input');
        filterInput.type = 'text';
        filterInput.placeholder = 'Filter by Site ID. You can enter multiple IDs separated by commas.';
        filterInput.className = 'form-control site-filter';
        filterInput.dataset.tableId = `table_${item}`; // Associate the filter with the specific table
        filterContainer.appendChild(filterInput);

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
            checkbox.dataset.tableId = `table_${item}`;
            checkbox.dataset.year = year;
            checkbox.style.marginLeft = '5px';

            const checkboxLabel = document.createElement('span');
            checkboxLabel.innerText = ` ${year} `;

            yearCheckboxContainer.appendChild(checkbox);
            yearCheckboxContainer.appendChild(checkboxLabel);
        }

        // Create the table inside the tab content
        const tableContainer = document.createElement('div');
        tableContainer.className = 'table-responsive';

        const table = document.createElement('table');
        table.className = 'table table-bordered w-100 text-center';
        table.id = `table_${item}`;

        const thead = document.createElement('thead');
        thead.className = 'table-primary';
        table.appendChild(thead);

        const tbody = document.createElement('tbody');
        table.appendChild(tbody);

        tableContainer.appendChild(table);
        tabContent.appendChild(filterContainer); // Add the filter input above the table
        tabContent.appendChild(yearCheckboxContainer); // Add the checkbox container above the table
        tabContent.appendChild(tableContainer);
        tabContentContainer.appendChild(tabContent);
    });
}



// Function to create table headers for a given table
function createTableHeaders(tableHead, minYear, maxYear) {
    tableHead.innerHTML = ''; // Clear existing headers
    const headerRow1 = document.createElement('tr');

    const siteIDTh = document.createElement('th');
    siteIDTh.innerText = 'Site ID';
    headerRow1.appendChild(siteIDTh);

    for (let year = minYear; year <= maxYear; year++) {
        const yearHeader = document.createElement('th');
        yearHeader.setAttribute('colspan', 12); // Each year has 12 months
        yearHeader.innerText = year;
        headerRow1.appendChild(yearHeader);
    }

    tableHead.appendChild(headerRow1);

    const headerRow2 = document.createElement('tr');
    const emptyTh = document.createElement('th'); // Placeholder for "Site ID"
    headerRow2.appendChild(emptyTh);

    for (let year = minYear; year <= maxYear; year++) {
        for (let month = 1; month <= 12; month++) {
            const monthHeader = document.createElement('th');
            monthHeader.innerText = month;
            headerRow2.appendChild(monthHeader);
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
            for (let month = 1; month <= 12; month++) {
                const cell = document.createElement('td');
                const yearData = dataForParameter[siteID]?.[year.toString()] || {};
                const cellValue = yearData[month.toString()] || 'n'; // Default to 'n' if not found

                cell.innerText = cellValue;

                // Add the green-cell class if the cell value is 'y'
                if (cellValue === 'y') {
                    cell.classList.add('green-cell');
                }

                row.appendChild(cell);
            }
        }

        tableBody.appendChild(row);
    });
}

// Event listener to handle clicks on sub-tabs
function setupTabListeners(tabType, items) {
    document.querySelectorAll(`#${tabType}SubTabs .nav-link`).forEach(tab => {
        tab.addEventListener('click', (event) => {
            const parameter = event.target.innerText.replace('SOP ', '');
            const tableID = `table_${parameter}`;
            const tableHead = document.getElementById(tableID).getElementsByTagName('thead')[0];
            const tableBody = document.getElementById(tableID).getElementsByTagName('tbody')[0];

            // Create table headers and populate body for the selected parameter/SOP
            createTableHeaders(tableHead, inventoryData[tabType].minYear, inventoryData[tabType].maxYear);
            populateTableBody(tabType, parameter, tableBody);
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

    // Calculate the number of columns per year (12 months)
    const columnsPerYear = 12;

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
        createTabsAndContent('general', generalSOPs);
        createTabsAndContent('logger', loggerParameters);

        // Set up tab listeners
        setupTabListeners('general', generalSOPs);
        setupTabListeners('logger', loggerParameters);

        // Automatically trigger the first tab's click event to populate it on page load
        document.getElementById('sop2-tab').click();
        document.getElementById('raw_chlorophyll-tab').click();
    }
    
    hideLoader(); 

});