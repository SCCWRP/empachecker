// Data for SOPs, years, and months
const sopCount = 13; // Number of SOPs
const years = [2021, 2022, 2023, 2024]; // Years to display
const months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]; // Months for each year

// Get table head and body elements
const tableHead = document.getElementById('example').getElementsByTagName('thead')[0];
const tableBody = document.getElementById('example').getElementsByTagName('tbody')[0];

const regionSiteMap = {
    North: ['NC-LA', 'NC-MC'],
    Central: ['CC-MLB', 'CC-ALA'],
    South: ['SC-LLA', 'SC-TOP']
};

// Function to dynamically create table headers for the general tab
function createTableHeaders() {
    const headerRow1 = document.createElement('tr');
    const regionTh = document.createElement('th');
    regionTh.setAttribute('rowspan', '3');
    regionTh.innerText = 'Region';
    headerRow1.appendChild(regionTh);

    const siteIDTh = document.createElement('th');
    siteIDTh.setAttribute('rowspan', '3');
    siteIDTh.innerText = 'SiteID';
    headerRow1.appendChild(siteIDTh);

    for (let sop = 1; sop <= sopCount; sop++) {
        const sopHeader = document.createElement('th');
        sopHeader.setAttribute('colspan', years.length * months.length);
        sopHeader.innerText = `SOP ${sop}`;
        headerRow1.appendChild(sopHeader);
    }

    tableHead.appendChild(headerRow1);

    const headerRow2 = document.createElement('tr');
    for (let sop = 1; sop <= sopCount; sop++) {
        years.forEach(year => {
            const yearHeader = document.createElement('th');
            yearHeader.setAttribute('colspan', months.length);
            yearHeader.innerText = year;
            headerRow2.appendChild(yearHeader);
        });
    }

    tableHead.appendChild(headerRow2);

    const headerRow3 = document.createElement('tr');
    for (let sop = 1; sop <= sopCount; sop++) {
        years.forEach(year => {
            months.forEach(month => {
                const monthHeader = document.createElement('th');
                monthHeader.innerText = month;
                headerRow3.appendChild(monthHeader);
            });
        });
    }

    tableHead.appendChild(headerRow3);
}

// Function to dynamically create table body rows for the general tab
// Function to dynamically create table body rows for the general tab
function createTableBody(regionSiteMap) {
    Object.keys(regionSiteMap).forEach(region => {
        const sites = regionSiteMap[region];

        sites.forEach(siteID => {
            const row = document.createElement('tr');

            if (sites.indexOf(siteID) === 0) {
                const regionTd = document.createElement('td');
                regionTd.setAttribute('rowspan', sites.length);
                regionTd.innerText = region;
                row.appendChild(regionTd);
            }

            const siteIDTd = document.createElement('td');
            siteIDTd.innerText = siteID;
            row.appendChild(siteIDTd);

            for (let sop = 1; sop <= sopCount; sop++) {
                years.forEach(year => {
                    months.forEach(month => {
                        const cell = document.createElement('td');
                        cell.innerText = ''; // Data can be inserted here
                        row.appendChild(cell);
                    });
                });
            }

            tableBody.appendChild(row);
        });
    });
}


// Call functions to generate the general table
createTableHeaders();
createTableBody(regionSiteMap);
