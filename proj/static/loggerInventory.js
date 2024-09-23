{
    // List of logger parameters based on your screenshot
    const loggerParameters = [
        'raw_chlorophyll', 'raw_conductivity', 'raw_depth', 'raw_do', 'raw_do_pct',
        'raw_h2otemp', 'raw_orp', 'raw_ph', 'raw_pressure', 'raw_qvalue',
        'raw_salinity', 'raw_turbidity'
    ];

    // Min and max years (you may have this set elsewhere, ensure it matches your requirements)
    const minYear = 2021;
    const maxYear = 2025;

    // Reference to sub-tabs and tab content containers
    const loggerSubTabs = document.getElementById('loggerSubTabs');
    const loggerTabContent = document.getElementById('loggerTabContent');

    // Function to create sub-tabs and content for each logger parameter
    function createLoggerTabsAndContent() {
        loggerParameters.forEach((parameter, index) => {
            // Create the tab header (sub-tab)
            const tabHeader = document.createElement('li');
            tabHeader.className = 'nav-item';
            tabHeader.setAttribute('role', 'presentation');

            const tabButton = document.createElement('button');
            tabButton.className = `nav-link ${index === 0 ? 'active' : ''}`;
            tabButton.id = `${parameter}-tab`;
            tabButton.setAttribute('data-bs-toggle', 'tab');
            tabButton.setAttribute('data-bs-target', `#${parameter}`);
            tabButton.setAttribute('type', 'button');
            tabButton.setAttribute('role', 'tab');
            tabButton.setAttribute('aria-controls', parameter);
            tabButton.setAttribute('aria-selected', index === 0 ? 'true' : 'false');
            tabButton.innerText = parameter;

            tabHeader.appendChild(tabButton);
            loggerSubTabs.appendChild(tabHeader);

            // Create the tab content container
            const tabContent = document.createElement('div');
            tabContent.className = `tab-pane fade ${index === 0 ? 'show active' : ''}`;
            tabContent.id = parameter;
            tabContent.setAttribute('role', 'tabpanel');
            tabContent.setAttribute('aria-labelledby', `${parameter}-tab`);

            // Create the table inside the tab content
            const tableContainer = document.createElement('div');
            tableContainer.className = 'table-responsive';

            const table = document.createElement('table');
            table.className = 'table table-bordered w-100 text-center';
            table.id = `table_${parameter}`;

            const thead = document.createElement('thead');
            thead.className = 'table-primary';
            table.appendChild(thead);

            const tbody = document.createElement('tbody');
            table.appendChild(tbody);

            tableContainer.appendChild(table);
            tabContent.appendChild(tableContainer);
            loggerTabContent.appendChild(tabContent);
        });
    }

    // Call the function to create tabs and content
    createLoggerTabsAndContent();



    // Sample inventory data for Logger Parameters (modify as needed)
    const inventoryDataLogger = {
        'general': {
            'raw_chlorophyll': {
                'CC-CAR': {
                    '2021': {'1': 'y', '2': 'n', '3': 'y'} // Continue with all months
                }
            },
            // Add other logger parameter data under 'general'
        }
    };


    // Function to create table headers for each logger parameter
    function createLoggerTableHeaders(tableHead) {
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

    // Function to populate the logger table body with data
    function populateLoggerTableBody(parameter, tableBody) {
        tableBody.innerHTML = ''; // Clear existing body rows

        const dataForParameter = inventoryDataLogger['general'][parameter] || {}; // Fetch data for the current logger parameter
        const sites = Object.keys(dataForParameter);

        sites.forEach(siteID => {
            const row = document.createElement('tr');

            const siteIDCell = document.createElement('td');
            siteIDCell.innerText = siteID;
            row.appendChild(siteIDCell);

            for (let year = minYear; year <= maxYear; year++) {
                for (let month = 1; month <= 12; month++) {
                    const cell = document.createElement('td');
                    const yearData = dataForParameter[siteID]?.[year.toString()] || {};
                    cell.innerText = yearData[month.toString()] || 'n'; // Default to 'n' if not found
                    row.appendChild(cell);
                }
            }

            tableBody.appendChild(row);
        });
    }

    // Adjust event listener to target your hard-coded logger sub-tabs
    document.querySelectorAll('#loggerSubTabs .nav-link').forEach(tab => {
        tab.addEventListener('click', (event) => {
            const parameter = event.target.innerText; // Extract logger parameter name
            const tableID = `table_${parameter}`;
            const tableHead = document.getElementById(tableID).getElementsByTagName('thead')[0];
            const tableBody = document.getElementById(tableID).getElementsByTagName('tbody')[0];

            // Create table headers and populate body for the selected logger parameter
            createLoggerTableHeaders(tableHead);
            populateLoggerTableBody(parameter, tableBody);
        });
    });

    // Pre-select and populate the first logger parameter tab on page load
    document.getElementById('raw_chlorophyll-tab').click();
}