{

    document.addEventListener('DOMContentLoaded', (event) => {
        // Add event listener to the logger tab
        const loggerTab = document.getElementById('logger-tab');
        
        if (loggerTab) {
            loggerTab.addEventListener('click', function() {
                alert('Logger Tab is under construction! Data and functionalies might not be correct/working.');
                return

            });
        }
    });
    

    let loggerChart; // Global variable to store Chart.js instance

    // Function to fetch logger data from the Flask backend
    async function fetchLoggerData() {
        try {
            const response = await fetch('/checker/get-inventory-data');
            if (!response.ok) {
                throw new Error('Network response was not ok');
            }
            const data = await response.json(); // Assuming Flask returns JSON
            return data;
        } catch (error) {
            console.error('There was a problem with the fetch operation:', error);
            return null;
        }
    }

    // Function to extract years from loggerData
    function getYears(loggerData) {
        const firstRegion = Object.keys(loggerData)[0];
        const firstSite = Object.keys(loggerData[firstRegion])[0];
        return Object.keys(loggerData[firstRegion][firstSite]).map(Number); // Extract years
    }

    // Function to extract months from loggerData
    function getMonths(loggerData, years) {
        const firstRegion = Object.keys(loggerData)[0];
        const firstSite = Object.keys(loggerData[firstRegion])[0];
        return Object.keys(loggerData[firstRegion][firstSite][years[0]]).map(Number); // Extract months
    }

    // Function to extract logger parameters from loggerData
    function getLoggerParameters(loggerData) {
        const firstRegion = Object.keys(loggerData)[0];
        const firstSite = Object.keys(loggerData[firstRegion])[0];
        const firstYear = Object.keys(loggerData[firstRegion][firstSite])[0];
        const firstMonth = Object.keys(loggerData[firstRegion][firstSite][firstYear])[0];
        return Object.keys(loggerData[firstRegion][firstSite][firstYear][firstMonth]);
    }

    // Function to dynamically create table headers
    function createLoggerTableHeaders(loggerData) {
        const tableHead = document.getElementById('loggerTable').getElementsByTagName('thead')[0];
        const years = getYears(loggerData); // Get the years dynamically
        const months = getMonths(loggerData, years); // Get the months dynamically
        const loggerParameters = getLoggerParameters(loggerData); // Get the logger parameters dynamically
        
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
        const tableBody = document.getElementById('loggerTable').getElementsByTagName('tbody')[0];
        const regions = Object.keys(loggerData); // Get regions

        regions.forEach(region => {
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

                const siteIDTd = document.createElement('td');
                siteIDTd.innerText = siteID;
                row.appendChild(siteIDTd);

                const years = getYears(loggerData); // Dynamically get the years
                const months = getMonths(loggerData, years); // Dynamically get the months
                const loggerParameters = getLoggerParameters(loggerData); // Dynamically get the logger parameters

                years.forEach(year => {
                    months.forEach(month => {
                        loggerParameters.forEach(param => {
                            const cell = document.createElement('td');
                            const loggerDataForMonth = loggerData[region][siteID][year]?.[month]?.[param];

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

    // Function to add click event listeners to table cells for logger inventory
    function addCellClickListenersForLoggerInventory(table, loggerData) {
        const rows = table.getElementsByTagName('tr');
        const loggerParameters = getLoggerParameters(loggerData); // Get logger parameters from top headers
        const months = getMonths(loggerData, getYears(loggerData)); // Get months
        const years = getYears(loggerData); // Get years
    
        // Iterate over the rows starting from index 2 (after the header rows)
        for (let i = 2; i < rows.length; i++) { 
            const row = rows[i];
            const cells = row.getElementsByTagName('td');
            let labelCount = 2; // Adjust for the first two columns (region and siteID)
    
            if (!row.cells[0].hasAttribute('rowspan')) {
                labelCount = 1;
            }
    
            // Iterate through the cells in the row
            for (let j = labelCount; j < cells.length; j++) {
                const cell = cells[j];
    
                // Only make the cell clickable if it contains 'y'
                if (cell.innerText === 'y') {
                    cell.style.cursor = 'pointer';
    
                    // Add click event listener
                    cell.addEventListener('click', function () {
                        const rowElement = this.parentElement;
                        let siteID = labelCount === 2 ? rowElement.cells[1].innerText : rowElement.cells[0].innerText;
                        let region = labelCount === 2 ? rowElement.cells[0].innerText : table.rows[rowElement.rowIndex - 1].cells[0].innerText;
    
                        const totalColumnsPerParam = months.length * years.length;
    
                        // Calculate correct indices for year, month, and parameter
                        const paramIndex = Math.floor((j - labelCount) / (months.length * years.length)); // Find the correct parameter
                        const yearIndex = Math.floor(((j - labelCount) % totalColumnsPerParam) / months.length); // Find the correct year
                        const monthIndex = (j - labelCount) % months.length; // Find the correct month
    
                        const year = years[yearIndex];
                        const month = months[monthIndex];
                        const parameter = loggerParameters[paramIndex]; // Get the correct parameter
    
                        console.log(`Fetching data for ${region}, ${siteID}, ${year}, ${month}, ${parameter}`);
                        showModalForLoggerInventory(region, siteID, year, month, parameter); // Pass correct parameter and month
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
                labels: data.map(d => new Date(d.x).toLocaleString()), // x-axis labels (samplecollectiontimestamp)
                datasets: [{
                    label: `${parameter} Data`, // Dataset label
                    data: data.map(d => d.y),   // y-axis data (parameter value)
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
                        type: 'time', // Indicate that the x-axis is a time series
                        time: {
                            unit: 'month', // You can adjust the time unit (e.g., 'day', 'week', 'month') depending on your data
                        },
                        title: {
                            display: true,
                            text: 'Sample Collection Timestamp'
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


    // Function to fetch real logger data for the graph
    async function fetchLoggerDataForGraph(region, siteID, year, month, parameter) {
        console.log(`Fetching data for ${region}, ${siteID}, ${year}, ${parameter}`);

        try {
            // Fetch data from the new API route
            const response = await fetch(`/checker/get-logger-graph-data?siteid=${siteID}&year=${year}&month=${month}&parameter=${parameter}`);
            
            if (!response.ok) {
                throw new Error('Network response was not ok');
            }
            
            const data = await response.json(); // Parse the returned JSON data

            if (data.error) {
                console.error(data.error);
                return [];
            }
            
            return data; // Return the fetched data to be used in the chart

        } catch (error) {
            console.error('There was a problem with the fetch operation:', error);
            return [];
        }
    }


    // Function to show modal with a Chart.js graph for the logger parameter and year
    async function showModalForLoggerInventory(region, siteID, year, month, parameter) {
        // Fetch the real data based on the clicked region, site, year, and parameter
        const data = await fetchLoggerDataForGraph(region, siteID, year, month, parameter);

        // Create the Chart.js graph with the fetched data, passing siteID and year for the title
        createChartJsGraph(data, parameter, siteID, year);

        // Show the modal with the Chart.js graph
        var modal = new bootstrap.Modal(document.getElementById('infoModal'));
        modal.show();
    }



    function fillEmptyCells() {
        const tableBody = document.getElementById('loggerTable').getElementsByTagName('tbody')[0];
        const rows = tableBody.getElementsByTagName('tr');
        
        for (let i = 0; i < rows.length; i++) {
            const cells = rows[i].getElementsByTagName('td');
            
            for (let j = 0; j < cells.length; j++) {
                if (cells[j].innerText.trim() === '') { // If cell is empty
                    cells[j].innerText = 'n'; // Set it to 'n'
                }
            }
        }
    }

    // Main function to fetch data and render the table
    async function renderLoggerTable() {

        const loggerData = await fetchLoggerData();
        console.log(loggerData);

        if (loggerData) {
            createLoggerTableHeaders(loggerData);
            createLoggerTableBody(loggerData);
            const loggerTable = document.getElementById('loggerTable');
            addCellClickListenersForLoggerInventory(loggerTable, loggerData); // Assuming this is defined elsewhere
            fillEmptyCells()
        }
    }

    // Call the main function to render the logger table
    renderLoggerTable();

    


}