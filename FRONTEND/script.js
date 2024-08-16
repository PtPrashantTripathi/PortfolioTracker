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
    const options = {
        series: [
            {
                name: "Investment",
                data: data.map((d) => ({
                    x: new Date(d.date),
                    y: parseFloat(d.holding),
                })),
            },
            {
                name: "Market Value",
                data: data.map((d) => ({
                    x: new Date(d.date),
                    y: parseFloat(d.close),
                })),
            },
        ],
        chart: {
            type: "area",
            toolbar: {
                show: false,
            },
        },
        dataLabels: {
            enabled: false,
        },
        stroke: {
            curve: "smooth",
            width: 2,
        },
        xaxis: {
            type: "datetime",
            axisBorder: {
                show: false,
            },
            axisTicks: {
                show: false,
            },
        },
        yaxis: {
            // tickAmount: 4,
            // floating: false,
            labels: {
                formatter: (a) => priceConvert(a, true),
                style: {
                    colors: "#8e8da4",
                },
            },
            axisBorder: {
                show: false,
            },
            axisTicks: {
                show: false,
            },
        },
        fill: {
            opacity: 0.25,
        },
        colors: ["#007bff", "#28a745"],
        tooltip: {
            x: {
                format: "yyyy-MM-dd",
            },
            y: {
                formatter: (a) => priceConvert(a),
            },
        },
    };

    const chart = new ApexCharts(
        document.getElementById("holdingsTrandsChart"),
        options
    );

    chart.render();

    return chart;
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
        row.appendChild(
            createCell(
                priceConvert(record.close_price),
                closePrice < avgPrice ? ["text-danger"] : ["text-success"]
            )
        );

        const currentAmount = priceConvert(record.close_amount);

        row.appendChild(
            createCell(
                currentAmount,
                parseFloat(record.close_amount) < 0
                    ? ["text-danger"]
                    : ["text-success"]
            )
        );

        const investedAmount = parseFloat(record.holding_amount);
        const pnl = parseFloat(record.close_amount) - investedAmount;
        const returnPercentage = parseNum((pnl * 100) / record.holding_amount);
        const pnlText = `${priceConvert(pnl)} (${
            pnl < 0 ? "+" : ""
        }${returnPercentage}%)`;

        row.appendChild(
            createCell(pnlText, pnl < 0 ? ["text-danger"] : ["text-success"])
        );

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
