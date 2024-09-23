{
    const minYear = 2021;
    const maxYear = 2025;

    // List of SOPs for the General Tab
    const generalSOPs = [2, 4, 6, 7, 8, 9, 10, 11, 13];

    // Reference to sub-tabs and tab content containers
    const generalSubTabs = document.getElementById('generalSubTabs');
    const generalTabContent = document.getElementById('generalTabContent');

    // Function to create sub-tabs and content for each SOP
    function createGeneralTabsAndContent() {
        generalSOPs.forEach((sop, index) => {
            // Create the tab header (sub-tab)
            const tabHeader = document.createElement('li');
            tabHeader.className = 'nav-item';
            tabHeader.setAttribute('role', 'presentation');

            const tabButton = document.createElement('button');
            tabButton.className = `nav-link ${index === 0 ? 'active' : ''}`;
            tabButton.id = `sop${sop}-tab`;
            tabButton.setAttribute('data-bs-toggle', 'tab');
            tabButton.setAttribute('data-bs-target', `#sop${sop}`);
            tabButton.setAttribute('type', 'button');
            tabButton.setAttribute('role', 'tab');
            tabButton.setAttribute('aria-controls', `sop${sop}`);
            tabButton.setAttribute('aria-selected', index === 0 ? 'true' : 'false');
            tabButton.innerText = `SOP ${sop}`;

            tabHeader.appendChild(tabButton);
            generalSubTabs.appendChild(tabHeader);

            // Create the tab content container
            const tabContent = document.createElement('div');
            tabContent.className = `tab-pane fade ${index === 0 ? 'show active' : ''}`;
            tabContent.id = `sop${sop}`;
            tabContent.setAttribute('role', 'tabpanel');
            tabContent.setAttribute('aria-labelledby', `sop${sop}-tab`);

            // Create the table inside the tab content
            const tableContainer = document.createElement('div');
            tableContainer.className = 'table-responsive';

            const table = document.createElement('table');
            table.className = 'table table-bordered w-100 text-center';
            table.id = `table_sop${sop}`;

            const thead = document.createElement('thead');
            thead.className = 'table-primary';
            table.appendChild(thead);

            const tbody = document.createElement('tbody');
            table.appendChild(tbody);

            tableContainer.appendChild(table);
            tabContent.appendChild(tableContainer);
            generalTabContent.appendChild(tabContent);
        });
    }

    // Call the function to create tabs and content
    createGeneralTabsAndContent();

    // Sample data structure for testing
    const inventoryDataGeneral = {
        'general': {
            'sop2': {
                'CC-CAR': {
                    '2021': {'1': 'y', '2': 'n', '3': 'y', '4': 'y', '5': 'n', '6': 'y'} // Sample data
                },
                'NC-SAC': {
                    '2022': {'1': 'y', '2': 'y', '3': 'n', '4': 'n', '5': 'y', '6': 'n'} // Sample data
                }
            },
            'sop4': {
                'CC-SD': {
                    '2021': {'1': 'y', '2': 'n', '3': 'n'} // Sample data
                }
            }
            // Add more SOPs under 'general'
        }
    };

    // Function to create table headers for each SOP
    function createGeneralTableHeaders(tableHead) {
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

    // Function to populate the SOP table body with data
    function populateGeneralTableBody(sopID, tableBody) {
        tableBody.innerHTML = ''; // Clear existing body rows

        const dataForSOP = inventoryDataGeneral['general'][`sop${sopID}`] || {}; // Fetch data for the current SOP
        const sites = Object.keys(dataForSOP);

        sites.forEach(siteID => {
            const row = document.createElement('tr');

            const siteIDCell = document.createElement('td');
            siteIDCell.innerText = siteID;
            row.appendChild(siteIDCell);

            for (let year = minYear; year <= maxYear; year++) {
                for (let month = 1; month <= 12; month++) {
                    const cell = document.createElement('td');
                    const yearData = dataForSOP[siteID]?.[year.toString()] || {};
                    cell.innerText = yearData[month.toString()] || 'n'; // Default to 'n' if not found
                    row.appendChild(cell);
                }
            }

            tableBody.appendChild(row);
        });
    }

    // Ensure event listeners are set correctly after dynamic creation
    document.querySelectorAll('#generalSubTabs').forEach(tabContainer => {
        tabContainer.addEventListener('click', (event) => {
            if (event.target && event.target.tagName === 'BUTTON') {
                const sopID = event.target.innerText.replace('SOP ', ''); // Extract SOP number
                const tableID = `table_sop${sopID}`;
                const tableHead = document.getElementById(tableID).getElementsByTagName('thead')[0];
                const tableBody = document.getElementById(tableID).getElementsByTagName('tbody')[0];

                // Create table headers and populate body for the selected SOP
                createGeneralTableHeaders(tableHead);
                populateGeneralTableBody(sopID, tableBody);
            }
        });
    });

    // Automatically trigger the first tab's click event to populate it on page load
    document.getElementById('sop2-tab').click();


}