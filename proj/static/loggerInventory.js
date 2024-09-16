let loggerChart; // Global variable to store Chart.js instance

// Example logger data with regions and sites embedded
const loggerData = {
    north: {
        'NC-LA': {
            2021: { DO: 'y', Temperature: 'n', pH: 'y', Depth: 'n' },
            2022: { DO: 'n', Temperature: 'y', pH: 'n', Depth: 'y' },
            2023: { DO: 'n', Temperature: 'y', pH: 'n', Depth: 'y' },
            2024: { DO: 'n', Temperature: 'y', pH: 'n', Depth: 'y' }

        },
        'NC-MC': {
            2021: { DO: 'y', Temperature: 'y', pH: 'n', Depth: 'n' },
            2022: { DO: 'y', Temperature: 'n', pH: 'y', Depth: 'y' },
            2023: { DO: 'n', Temperature: 'y', pH: 'n', Depth: 'y' },
            2024: { DO: 'n', Temperature: 'y', pH: 'n', Depth: 'y' }
        }
    },
    central: {
        'CC-LA': {
            2021: { DO: 'n', Temperature: 'y', pH: 'n', Depth: 'y' },
            2022: { DO: 'y', Temperature: 'y', pH: 'y', Depth: 'n' },
            2023: { DO: 'n', Temperature: 'y', pH: 'n', Depth: 'y' },
            2024: { DO: 'n', Temperature: 'y', pH: 'n', Depth: 'y' }
        },
        'CC-SD': {
            2021: { DO: 'n', Temperature: 'y', pH: 'n', Depth: 'y' },
            2022: { DO: 'y', Temperature: 'y', pH: 'y', Depth: 'n' },
            2023: { DO: 'n', Temperature: 'y', pH: 'n', Depth: 'y' },
            2024: { DO: 'n', Temperature: 'y', pH: 'n', Depth: 'y' }
        }
    },
    south: {
        'SC-LLA': {
            2021: { DO: 'n', Temperature: 'n', pH: 'y', Depth: 'y' },
            2022: { DO: 'y', Temperature: 'n', pH: 'n', Depth: 'n' },
            2023: { DO: 'n', Temperature: 'y', pH: 'n', Depth: 'y' },
            2024: { DO: 'n', Temperature: 'y', pH: 'n', Depth: 'y' }
        },
        'SC-MAA': {
            2021: { DO: 'n', Temperature: 'n', pH: 'y', Depth: 'y' },
            2022: { DO: 'y', Temperature: 'n', pH: 'n', Depth: 'n' },
            2023: { DO: 'n', Temperature: 'y', pH: 'n', Depth: 'y' },
            2024: { DO: 'n', Temperature: 'y', pH: 'n', Depth: 'y' }
        }
    }
};
{
    // Function to extract logger parameters from loggerData
    function getLoggerParameters(loggerData) {
        // Grab the first region, siteID, and year and extract the parameter keys
        const firstRegion = Object.keys(loggerData)[0];
        const firstSite = Object.keys(loggerData[firstRegion])[0];
        const firstYear = Object.keys(loggerData[firstRegion][firstSite])[0];
        return Object.keys(loggerData[firstRegion][firstSite][firstYear]);
    }

    // Extract years from loggerData
    const years = Array.from(
        new Set(
            Object.values(loggerData)
                .flatMap(region => Object.values(region))
                .flatMap(site => Object.keys(site))
        )
    ).map(year => parseInt(year));

    // Extract logger parameters from loggerData
    const loggerParameters = getLoggerParameters(loggerData);

    // Get logger table head and body elements
    const loggerTableHead = document.getElementById('loggerTable').getElementsByTagName('thead')[0];
    const loggerTableBody = document.getElementById('loggerTable').getElementsByTagName('tbody')[0];

    // Function to dynamically create logger table headers
    function createLoggerTableHeaders() {
        const headerRow1 = document.createElement('tr');
        const regionTh = document.createElement('th');
        regionTh.setAttribute('rowspan', '2');
        regionTh.innerText = 'Region';
        headerRow1.appendChild(regionTh);

        const siteIDTh = document.createElement('th');
        siteIDTh.setAttribute('rowspan', '2');
        siteIDTh.innerText = 'SiteID';
        headerRow1.appendChild(siteIDTh);

        years.forEach(year => {
            const yearHeader = document.createElement('th');
            yearHeader.setAttribute('colspan', loggerParameters.length);
            yearHeader.innerText = year;
            headerRow1.appendChild(yearHeader);
        });

        loggerTableHead.appendChild(headerRow1);

        const headerRow2 = document.createElement('tr');
        years.forEach(() => {
            loggerParameters.forEach(param => {
                const paramHeader = document.createElement('th');
                paramHeader.innerText = param;
                headerRow2.appendChild(paramHeader);
            });
        });

        loggerTableHead.appendChild(headerRow2);
    }

    // Function to dynamically create logger table body rows with conditional coloring from loggerData
    function createLoggerTableBody(loggerData) {
        // Iterate through regions
        Object.keys(loggerData).forEach(region => {
            const sites = Object.keys(loggerData[region]);

            sites.forEach(siteID => {
                const row = document.createElement('tr');

                // Add the region column for the first site in the region
                if (sites.indexOf(siteID) === 0) {
                    const regionTd = document.createElement('td');
                    regionTd.setAttribute('rowspan', sites.length);
                    regionTd.innerText = region.charAt(0).toUpperCase() + region.slice(1); // Capitalize region
                    row.appendChild(regionTd);
                }

                // Add the siteID column
                const siteIDTd = document.createElement('td');
                siteIDTd.innerText = siteID;
                row.appendChild(siteIDTd);

                // Loop through years and loggerParameters
                years.forEach(year => {
                    const loggerDataForYear = loggerData[region][siteID][year];

                    if (loggerDataForYear) {
                        loggerParameters.forEach(param => {
                            const cell = document.createElement('td');

                            // Insert the 'y' or 'n' value, if available
                            const value = loggerDataForYear[param];
                            if (value !== undefined) {
                                cell.innerText = value;

                                // Color the cell green if value is 'y'
                                if (value === 'y') {
                                    cell.style.backgroundColor = 'green';
                                    cell.style.color = 'white'; // Optional: Make text white for visibility
                                }
                            } else {
                                cell.innerText = ''; // Empty if no data
                            }

                            row.appendChild(cell);
                        });
                    } else {
                        // If no data for the year, add empty cells for each parameter
                        loggerParameters.forEach(() => {
                            const cell = document.createElement('td');
                            cell.innerText = ''; // No data
                            row.appendChild(cell);
                        });
                    }
                });

                // Append the row to the table body
                loggerTableBody.appendChild(row);
            });
        });
    }

    // Function to add click event listeners to table cells for logger inventory
    function addCellClickListenersForLoggerInventory(table) {
        const rows = table.getElementsByTagName('tr');
        for (let i = 2; i < rows.length; i++) { // Start after header rows
            const row = rows[i];
            const cells = row.getElementsByTagName('td');
            let labelCount = 2;

            if (!row.cells[0].hasAttribute('rowspan')) {
                labelCount = 1;
            }

            for (let j = labelCount; j < cells.length; j++) {
                const cell = cells[j];
                
                // Only make the cell clickable if it contains 'y'
                if (cell.innerText === 'y') {
                    cell.style.cursor = 'pointer';

                    cell.addEventListener('click', function () {
                        const rowElement = this.parentElement;
                        let siteID = labelCount === 2 ? rowElement.cells[1].innerText : rowElement.cells[0].innerText;
                        let region = labelCount === 2 ? rowElement.cells[0].innerText : table.rows[rowElement.rowIndex - 1].cells[0].innerText;

                        const yearIndex = Math.floor((j - labelCount) / loggerParameters.length);
                        const paramIndex = (j - labelCount) % loggerParameters.length;

                        const year = years[yearIndex];
                        const parameter = loggerParameters[paramIndex];

                        showModalForLoggerInventory(region, siteID, year, parameter);
                    });
                }
            }
        }
    }


    // Function to create a Chart.js line chart for the logger data
    function createChartJsGraph(data, parameter, siteID, year) {
        const ctx = document.getElementById('loggerChart').getContext('2d');

        // Destroy previous chart instance if it exists
        if (loggerChart) {
            loggerChart.destroy();
        }

        // Create a new chart instance
        loggerChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: data.map(d => `Month ${d.x}`), // x-axis labels (e.g., Month 1, Month 2)
                datasets: [{
                    label: `${parameter} Data`, // Dataset label
                    data: data.map(d => d.y),   // y-axis data
                    backgroundColor: 'rgba(75, 192, 192, 0.2)',
                    borderColor: 'rgba(75, 192, 192, 1)',
                    borderWidth: 1,
                    fill: true
                }]
            },
            options: {
                responsive: true,
                plugins: {
                    title: {
                        display: true,
                        text: `${siteID} - ${year}`  // Set title as {SiteID}-{Year}
                    }
                },
                scales: {
                    x: {
                        title: {
                            display: true,
                            text: 'Month'
                        }
                    },
                    y: {
                        title: {
                            display: true,
                            text: parameter
                        },
                        beginAtZero: true
                    }
                }
            }
        });
    }


    // Function to simulate fetching logger data for the graph
    function fetchLoggerData(region, siteID, year, parameter) {
        // Simulated data (You would fetch actual data here)
        const data = [];
        for (let i = 0; i < 12; i++) {
            data.push({ x: i + 1, y: Math.random() * 100 }); // x: Month, y: Random value
        }
        return data;
    }

    // Function to show modal with a Chart.js graph for the logger parameter and year
    // Function to show modal with a Chart.js graph for the logger parameter and year
    function showModalForLoggerInventory(region, siteID, year, parameter) {
        // Fetch the data based on the clicked region, site, year, and parameter
        const data = fetchLoggerData(region, siteID, year, parameter);

        // Create the Chart.js graph with the fetched data, passing siteID and year for the title
        createChartJsGraph(data, parameter, siteID, year);

        // Show the modal with the Chart.js graph
        var modal = new bootstrap.Modal(document.getElementById('infoModal'));
        modal.show();
    }


    // Call functions to generate the logger table
    createLoggerTableHeaders();
    createLoggerTableBody(loggerData);

    // Add click event listener to the logger inventory table
    const loggerTable = document.getElementById('loggerTable');
    addCellClickListenersForLoggerInventory(loggerTable);
}