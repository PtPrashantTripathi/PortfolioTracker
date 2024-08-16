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

async function loadHoldingsTrandsChart(data) {
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
                    borderColor: "#007bff",
                    backgroundColor: "#007bff",
                    borderWidth: 2,
                    fill: false,
                    tension: 1,
                },
                {
                    label: "Market Value",
                    data: data.map((d) => parseFloat(d.close)),
                    borderColor: "#28a745",
                    backgroundColor: "#28a745",
                    borderWidth: 2,
                    fill: {
                        target: "origin",
                        above: "#28a74570",
                    },
                    tension: 1,
                },
            ],
        },
        options: {
            responsive: true,
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

function priceConvert(value, short = false) {
    const amount = parseFloat(value);

    if (short) {
        if (amount >= 1e7) return `₹${(amount / 1e7).toFixed(2)}Cr`;
        if (amount >= 1e5) return `₹${(amount / 1e5).toFixed(2)}L`;
        if (amount >= 1e3) return `₹${(amount / 1e3).toFixed(2)}K`;
    }

    return amount.toLocaleString("en-IN", {
        maximumFractionDigits: 2,
        style: "currency",
        currency: "INR",
    });
}

function parseNum(value) {
    return parseFloat(value) === parseInt(value)
        ? parseInt(value)
        : parseFloat(parseFloat(value).toFixed(2));
}
// Helper function to create a cell with text content and optional classes
function createCell(text, classes = []) {
    const cell = document.createElement("td");
    cell.innerHTML = text;
    classes.forEach((cls) => cell.classList.add(cls));
    return cell;
}

async function loadHoldingsDataTable(data) {
    const tableBody = document.getElementById("CurrentHoldingsTable");
    tableBody.innerHTML = ""; // Clear existing table rows

    data.forEach((record) => {
        const row = document.createElement("tr");

        // Create and append cells
        row.appendChild(createCell(record.segment));
        row.appendChild(createCell(record.symbol));
        row.appendChild(createCell(parseNum(record.holding_quantity)));
        row.appendChild(createCell(priceConvert(record.avg_price)));
        row.appendChild(createCell(priceConvert(record.holding_amount)));

        const closePrice = parseFloat(record.close_price);
        const avgPrice = parseFloat(record.avg_price);
        const ltpClasses =
            closePrice < avgPrice ? ["text-danger"] : ["text-success"];
        row.appendChild(
            createCell(priceConvert(record.close_price), ltpClasses)
        );

        const currentAmount = priceConvert(record.close_amount);
        const currentAmountClass =
            parseFloat(record.close_amount) < 0
                ? ["text-danger"]
                : ["text-success"];
        row.appendChild(createCell(currentAmount, currentAmountClass));

        const investedAmount = parseFloat(record.holding_amount);
        const pnl = parseFloat(record.close_amount) - investedAmount;
        const pnlClasses = pnl < 0 ? ["text-danger"] : ["text-success"];
        const pnlText =
            pnl < 0
                ? `<b>${priceConvert(pnl)}</b>`
                : `<b>+${priceConvert(pnl)}</b>`;
        row.appendChild(createCell(pnlText, pnlClasses));

        // Append the row to the table body
        tableBody.appendChild(row);
    });
}

function updateLatestData(data) {
    const { close, holding } = data;
    const pnl = close - holding;
    const returnPercentage = parseNum((pnl * 100) / holding);
    const isProfit = pnl >= 0;

    const investedAmountElem = document.getElementById("invested_amount");
    const assetsWorthElem = document.getElementById("assets_worth");
    const overallReturnElem = document.getElementById("overall_return");

    // Update the invested amount
    investedAmountElem.textContent = priceConvert(holding, true);

    // Update the asset worth
    assetsWorthElem.textContent = priceConvert(close, true);

    // Update the asset overall return
    const formattedPnl = priceConvert(pnl, true);
    const formattedReturnPercentage = `${
        isProfit ? "+" : ""
    }${returnPercentage}%`;
    overallReturnElem.textContent = `${formattedPnl} (${formattedReturnPercentage})`;

    // Apply the appropriate status classes
    const statusClass = isProfit ? "text-success" : "text-danger";
    investedAmountElem.className = "text-primary";
    assetsWorthElem.className = statusClass;
    overallReturnElem.className = statusClass;
}

async function main() {
    // Fetch the data using getData
    const holdingsTrandsFilePath =
        "../DATA/GOLD/Holdings/HoldingsTrands_data.csv";
    const holdingsTrands_data = await getData(holdingsTrandsFilePath);
    loadHoldingsTrandsChart(holdingsTrands_data);

    const latestData = findMaxDateRecords(holdingsTrands_data)[0];
    updateLatestData(latestData);

    // Fetch the data using getData
    const holdingsDataFilePath = "../DATA/GOLD/Holdings/Holdings_data.csv";
    const holdingsData = await getData(holdingsDataFilePath);
    const filterdData = findMaxDateRecords(holdingsData);
    loadHoldingsDataTable(filterdData);
}

main();
