
let loggerChart; // Global variable to store Chart.js instance

// Example logger data with parameters as top-level keys
const loggerData = {
    DO: {
        north: {
            'NC-ABC': {
                2021: { 1: 'y', 2: 'n', 3: 'y', 4: 'y', 5: 'n', 6: 'y', 7: 'n', 8: 'y', 9: 'n', 10: 'y', 11: 'n', 12: 'y' },
                2022: { 1: 'n', 2: 'y', 3: 'n', 4: 'y', 5: 'y', 6: 'n', 7: 'y', 8: 'n', 9: 'y', 10: 'n', 11: 'y', 12: 'n' }
            },
            'NC-XYZ': {
                2021: { 1: 'y', 2: 'y', 3: 'y', 4: 'n', 5: 'y', 6: 'n', 7: 'y', 8: 'n', 9: 'y', 10: 'n', 11: 'y', 12: 'y' },
                2022: { 1: 'n', 2: 'n', 3: 'y', 4: 'n', 5: 'y', 6: 'n', 7: 'n', 8: 'y', 9: 'n', 10: 'y', 11: 'y', 12: 'n' }
            }
        },
        central: {
            'CC-LA': {
                2021: { 1: 'y', 2: 'n', 3: 'y', 4: 'n', 5: 'y', 6: 'n', 7: 'y', 8: 'n', 9: 'y', 10: 'n', 11: 'y', 12: 'n' },
                2022: { 1: 'y', 2: 'y', 3: 'y', 4: 'y', 5: 'n', 6: 'y', 7: 'n', 8: 'n', 9: 'y', 10: 'n', 11: 'n', 12: 'y' }
            },
            'CC-XYZ': {
                2021: { 1: 'n', 2: 'y', 3: 'y', 4: 'n', 5: 'y', 6: 'n', 7: 'y', 8: 'n', 9: 'y', 10: 'n', 11: 'n', 12: 'y' },
                2022: { 1: 'y', 2: 'y', 3: 'n', 4: 'y', 5: 'n', 6: 'y', 7: 'y', 8: 'n', 9: 'y', 10: 'n', 11: 'y', 12: 'y' }
            }
        },
        south: {
            'SC-LLA': {
                2021: { 1: 'n', 2: 'y', 3: 'n', 4: 'n', 5: 'y', 6: 'n', 7: 'y', 8: 'n', 9: 'y', 10: 'n', 11: 'y', 12: 'y' },
                2022: { 1: 'y', 2: 'n', 3: 'y', 4: 'n', 5: 'n', 6: 'y', 7: 'n', 8: 'y', 9: 'n', 10: 'y', 11: 'n', 12: 'y' }
            },
            'SC-XYZ': {
                2021: { 1: 'y', 2: 'n', 3: 'y', 4: 'n', 5: 'y', 6: 'y', 7: 'n', 8: 'y', 9: 'n', 10: 'y', 11: 'n', 12: 'y' },
                2022: { 1: 'n', 2: 'y', 3: 'n', 4: 'y', 5: 'n', 6: 'n', 7: 'y', 8: 'n', 9: 'y', 10: 'n', 11: 'y', 12: 'n' }
            }
        }
    },
    Temperature: {
        north: {
            'NC-ABC': {
                2021: { 1: 'n', 2: 'n', 3: 'y', 4: 'y', 5: 'n', 6: 'n', 7: 'y', 8: 'n', 9: 'n', 10: 'y', 11: 'n', 12: 'y' },
                2022: { 1: 'y', 2: 'y', 3: 'y', 4: 'n', 5: 'y', 6: 'n', 7: 'y', 8: 'n', 9: 'y', 10: 'n', 11: 'y', 12: 'n' }
            },
            'NC-XYZ': {
                2021: { 1: 'n', 2: 'n', 3: 'n', 4: 'y', 5: 'n', 6: 'y', 7: 'y', 8: 'n', 9: 'y', 10: 'n', 11: 'n', 12: 'y' },
                2022: { 1: 'y', 2: 'y', 3: 'y', 4: 'y', 5: 'n', 6: 'y', 7: 'y', 8: 'n', 9: 'y', 10: 'n', 11: 'n', 12: 'y' }
            }
        },
        central: {
            'CC-LA': {
                2021: { 1: 'y', 2: 'n', 3: 'y', 4: 'n', 5: 'n', 6: 'y', 7: 'y', 8: 'n', 9: 'y', 10: 'y', 11: 'n', 12: 'n' },
                2022: { 1: 'y', 2: 'y', 3: 'n', 4: 'n', 5: 'y', 6: 'y', 7: 'n', 8: 'y', 9: 'n', 10: 'y', 11: 'n', 12: 'y' }
            },
            'CC-XYZ': {
                2021: { 1: 'n', 2: 'n', 3: 'y', 4: 'n', 5: 'n', 6: 'y', 7: 'y', 8: 'n', 9: 'y', 10: 'n', 11: 'n', 12: 'y' },
                2022: { 1: 'y', 2: 'n', 3: 'y', 4: 'n', 5: 'n', 6: 'n', 7: 'y', 8: 'n', 9: 'y', 10: 'y', 11: 'n', 12: 'n' }
            }
        },
        south: {
            'SC-LLA': {
                2021: { 1: 'n', 2: 'y', 3: 'y', 4: 'y', 5: 'n', 6: 'y', 7: 'n', 8: 'n', 9: 'y', 10: 'n', 11: 'y', 12: 'y' },
                2022: { 1: 'y', 2: 'y', 3: 'n', 4: 'y', 5: 'n', 6: 'y', 7: 'n', 8: 'y', 9: 'n', 10: 'y', 11: 'n', 12: 'y' }
            },
            'SC-XYZ': {
                2021: { 1: 'n', 2: 'y', 3: 'y', 4: 'n', 5: 'n', 6: 'y', 7: 'y', 8: 'n', 9: 'y', 10: 'n', 11: 'y', 12: 'y' },
                2022: { 1: 'y', 2: 'y', 3: 'n', 4: 'y', 5: 'n', 6: 'y', 7: 'n', 8: 'y', 9: 'n', 10: 'y', 11: 'n', 12: 'n' }
            }
        }
    }
};



{
    // Function to extract years from loggerData
    function getYears(loggerData) {
        const firstParam = Object.keys(loggerData)[0]; // Get the first parameter, e.g., 'DO'
        const firstRegion = Object.keys(loggerData[firstParam])[0]; // Get the first region
        const firstSite = Object.keys(loggerData[firstParam][firstRegion])[0]; // Get the first site
        return Object.keys(loggerData[firstParam][firstRegion][firstSite]).map(Number); // Extract years
    }

    // Function to extract months from loggerData (assumes months are consistent across years)
    function getMonths(loggerData, years) {
        const firstParam = Object.keys(loggerData)[0];
        const firstRegion = Object.keys(loggerData[firstParam])[0];
        const firstSite = Object.keys(loggerData[firstParam][firstRegion])[0];
        return Object.keys(loggerData[firstParam][firstRegion][firstSite][years[0]]).map(Number);
    }

    // Extract years and months dynamically from loggerData
    const years = getYears(loggerData);
    const months = getMonths(loggerData, years);

    // Get the logger parameters from the top level of loggerData
    const loggerParameters = Object.keys(loggerData);

    // Get table head and body elements
    const tableHead = document.getElementById('loggerTable').getElementsByTagName('thead')[0];
    const tableBody = document.getElementById('loggerTable').getElementsByTagName('tbody')[0];

    // Function to dynamically create table headers
    function createLoggerTableHeaders() {
        const headerRow1 = document.createElement('tr');
        const regionTh = document.createElement('th');
        regionTh.setAttribute('rowspan', '3');
        regionTh.innerText = 'Region';
        headerRow1.appendChild(regionTh);

        const siteIDTh = document.createElement('th');
        siteIDTh.setAttribute('rowspan', '3');
        siteIDTh.innerText = 'SiteID';
        headerRow1.appendChild(siteIDTh);

        loggerParameters.forEach(param => {
            const paramHeader = document.createElement('th');
            paramHeader.setAttribute('colspan', years.length * months.length);
            paramHeader.innerText = param;
            headerRow1.appendChild(paramHeader);
        });

        tableHead.appendChild(headerRow1);

        const headerRow2 = document.createElement('tr');
        loggerParameters.forEach(() => {
            years.forEach(year => {
                const yearHeader = document.createElement('th');
                yearHeader.setAttribute('colspan', months.length);
                yearHeader.innerText = year;
                headerRow2.appendChild(yearHeader);
            });
        });

        tableHead.appendChild(headerRow2);

        const headerRow3 = document.createElement('tr');
        loggerParameters.forEach(() => {
            years.forEach(() => {
                months.forEach(month => {
                    const monthHeader = document.createElement('th');
                    monthHeader.innerText = month;
                    headerRow3.appendChild(monthHeader);
                });
            });
        });

        tableHead.appendChild(headerRow3);
    }

    // Function to dynamically create table body rows
    function createLoggerTableBody(loggerData) {
        const regions = Object.keys(loggerData[loggerParameters[0]]); // Get regions from the first parameter

        regions.forEach(region => {
            const sites = Object.keys(loggerData[loggerParameters[0]][region]);

            sites.forEach(siteID => {
                const row = document.createElement('tr');

                // Add the region column for the first site in the region
                if (sites.indexOf(siteID) === 0) {
                    const regionTd = document.createElement('td');
                    regionTd.setAttribute('rowspan', sites.length);
                    regionTd.innerText = region.charAt(0).toUpperCase() + region.slice(1); // Capitalize region
                    row.appendChild(regionTd);
                }

                const siteIDTd = document.createElement('td');
                siteIDTd.innerText = siteID;
                row.appendChild(siteIDTd);

                loggerParameters.forEach(param => {
                    years.forEach(year => {
                        months.forEach(month => {
                            const cell = document.createElement('td');
                            const loggerDataForMonth = loggerData[param][region][siteID][year]?.[month];

                            if (loggerDataForMonth) {
                                cell.innerText = loggerDataForMonth;

                                if (loggerDataForMonth === 'y') {
                                    cell.style.backgroundColor = 'green';
                                    cell.style.color = 'white'; // Make text white for visibility
                                }
                            } else {
                                cell.innerText = ''; // No data
                            }

                            row.appendChild(cell);
                        });
                    });
                });

                tableBody.appendChild(row);
            });
        });
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
    function showModalForLoggerInventory(region, siteID, year, parameter) {
        // Fetch the data based on the clicked region, site, year, and parameter
        const data = fetchLoggerData(region, siteID, year, parameter);

        // Create the Chart.js graph with the fetched data, passing siteID and year for the title
        createChartJsGraph(data, parameter, siteID, year);

        // Show the modal with the Chart.js graph
        var modal = new bootstrap.Modal(document.getElementById('infoModal'));
        modal.show();
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

                        const yearIndex = Math.floor((j - labelCount) / months.length);
                        const paramIndex = (j - labelCount) % loggerParameters.length;

                        const year = years[yearIndex];
                        const parameter = loggerParameters[paramIndex];

                        showModalForLoggerInventory(region, siteID, year, parameter);
                    });
                }
            }
        }
    }

    // Call functions to generate the logger table
    createLoggerTableHeaders();
    createLoggerTableBody(loggerData);

    // Add click event listener to the logger inventory table
    const loggerTable = document.getElementById('loggerTable');
    addCellClickListenersForLoggerInventory(loggerTable);
}