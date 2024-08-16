// Function to parse CSV data
function parseCSV(csv) {
    const lines = csv.trim().split("\n");
    const headers = lines[0].split(",").map((header) => header.trim()); // Trim all headers
    const result = [];

    for (let i = 1; i < lines.length; i++) {
        const obj = {};
        const currentLine = lines[i].split(",");

        headers.forEach((header, index) => {
            obj[header] = currentLine[index].trim();
        });

        result.push(obj);
    }

    return result;
}

// Function to get data
async function getData(filePath) {
    const response = await fetch(filePath);
    const csvData = await response.text(); // Await the fetched CSV data
    const parsedData = parseCSV(csvData); // Parse the CSV data
    return parsedData;
}

async function loadHoldingsTrandsChart() {
    // Fetch the data using getData
    const filePath = "../DATA/GOLD/Holdings/HoldingsTrands_data.csv";
    const data = await getData(filePath);

    const chartHolder = document
        .getElementById("holdingsTrandsChart")
        .getContext("2d");

    // Initialize and render the chart
    return new Chart(chartHolder, {
        type: "line",
        data: {
            labels: data.map((d) => new Date(d.date)),
            datasets: [
                {
                    label: "Investment",
                    data: data.map((d) => parseFloat(d.holding)),
                    borderColor: "rgba(70,130,180)",
                    backgroundColor: "rgba(70,130,180)",
                    borderWidth: 2,
                    fill: false,
                    tension: 1,
                },
                {
                    label: "Market Value",
                    data: data.map((d) => parseFloat(d.close)),
                    borderColor: "rgb(0,208,156)",
                    backgroundColor: "rgb(0,208,156)",
                    borderWidth: 2,
                    fill: {
                        target: "origin",
                        above: "rgb(0,208,156,0.5)",
                    },
                    tension: 1,
                },
            ],
        },
        options: {
            elements: {
                point: {
                    pointStyle: true,
                    pointRadius: 0,
                    pointHoverRadius: 5,
                },
            },
            scales: {
                x: {
                    type: "time",
                    time: {
                        unit: "quarter",
                        tooltipFormat: "yyyy-MM-dd",
                    },
                    title: {
                        display: true,
                        text: "Date (Quarterly)",
                    },
                },
                y: {
                    title: {
                        display: true,
                        text: "Price (INR)",
                    },
                    beginAtZero: false,
                },
            },
        },
    });
}

// Function to find records with the maximum date
function findMaxDateRecords(data) {
    const maxDate = data.reduce((max, item) => {
        const currentDate = new Date(item.date);
        return currentDate > max ? currentDate : max;
    }, new Date(data[0].date));

    return data.filter(
        (item) => new Date(item.date).getTime() === maxDate.getTime()
    );
}

function priceConvert(value) {
    return parseFloat(value).toLocaleString("en-IN", {
        maximumFractionDigits: 2,
        style: "currency",
        currency: "INR",
    });
}

async function loadHoldingsDataTable() {
    // Fetch the data using getData
    const filePath = "../DATA/GOLD/Holdings/Holdings_data.csv";
    const rawData = await getData(filePath);
    const filterdData = findMaxDateRecords(rawData);

    const tableBody = document.getElementById("CurrentHoldingsTable");
    tableBody.innerHTML = ""; // Clear existing table rows

    filterdData.forEach((record) => {
        const row = document.createElement("tr");

        // Segment column
        const segmentCell = document.createElement("td");
        segmentCell.textContent = record.segment;
        row.appendChild(segmentCell);

        // Stock Name column
        const stockName = document.createElement("td");
        stockName.textContent = record.symbol;
        row.appendChild(stockName);

        // Quantity column
        const quantityCell = document.createElement("td");
        quantityCell.textContent =
            parseFloat(record.holding_quantity) ===
            parseInt(record.holding_quantity)
                ? parseInt(record.holding_quantity)
                : parseFloat(record.holding_quantity);

        row.appendChild(quantityCell);

        // Avg Price column
        const avgPriceCell = document.createElement("td");
        avgPriceCell.textContent = priceConvert(record.avg_price);
        row.appendChild(avgPriceCell);

        // Invested Amount column
        const investedAmountCell = document.createElement("td");
        investedAmountCell.textContent = priceConvert(record.holding_amount);
        row.appendChild(investedAmountCell);

        // Last Traded Price column
        const ltpCell = document.createElement("td");
        ltpCell.textContent = priceConvert(record.close_price);
        ltpCell.classList.add("text-success");
        if (parseFloat(record.close_price) < parseFloat(record.avg_price)) {
            ltpCell.classList.add("text-danger");
        }
        row.appendChild(ltpCell);

        // Current Amount column
        const currentAmountCell = document.createElement("td");
        const currentAmount = priceConvert(record.close_amount);
        currentAmountCell.textContent = currentAmount;
        currentAmountCell.classList.add("text-success");
        if (currentAmount < 0) {
            currentAmountCell.classList.add("text-danger");
        }
        row.appendChild(currentAmountCell);

        // PNL column = Current Amount - Invested Amount
        const pnlCell = document.createElement("td");
        const pnl =
            parseFloat(record.close_amount) - parseFloat(record.holding_amount);
        if (pnl < 0) {
            pnlCell.innerHTML = "<b>" + priceConvert(pnl) + "</b>";
            pnlCell.classList.add("text-danger");
        } else {
            pnlCell.innerHTML = "<b>+" + priceConvert(pnl) + "</b>";
            pnlCell.classList.add("text-success");
        }
        row.appendChild(pnlCell);

        // Append the row to the table body
        tableBody.appendChild(row);
    });
}

function main() {
    loadHoldingsTrandsChart();
    loadHoldingsDataTable();
}

main();
