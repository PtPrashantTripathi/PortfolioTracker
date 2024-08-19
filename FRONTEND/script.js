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

async function loadProfitLossDataTable(data) {
    const tableBody = document.getElementById("ProfitLossTable");
    tableBody.innerHTML = ""; // Clear existing table rows

    data.forEach((record) => {
        const row = document.createElement("tr");

        const pnl =
            parseFloat(record.close_amount) - parseFloat(record.holding_amount);

        // Create and append cells
        row.appendChild(createCell(record.segment));
        row.appendChild(createCell(record.symbol));
        row.appendChild(createCell(parseNum(record.holding_quantity)));
        row.appendChild(createCell(priceConvert(record.avg_price)));
        row.appendChild(createCell(priceConvert(record.holding_amount)));

        row.appendChild(
            createCell(
                priceConvert(record.close_price),
                pnl < 0 ? ["text-danger"] : ["text-success"]
            )
        );

        row.appendChild(
            createCell(
                priceConvert(record.close_amount),
                pnl < 0 ? ["text-danger"] : ["text-success"]
            )
        );

        row.appendChild(
            createCell(
                `${priceConvert(pnl)} (${pnl < 0 ? "+" : ""}${parseNum(
                    (pnl * 100) / record.holding_amount
                )}%)`,
                pnl < 0 ? ["text-danger"] : ["text-success"]
            )
        );
        // Append the row to the table body
        tableBody.appendChild(row);
    });
}

async function loadHoldingsDataTable(data) {
    const tableBody = document.getElementById("CurrentHoldingsTable");
    tableBody.innerHTML = ""; // Clear existing table rows

    data.forEach((record) => {
        const row = document.createElement("tr");

        const pnl =
            parseFloat(record.close_amount) - parseFloat(record.holding_amount);

        // Create and append cells
        row.appendChild(createCell(record.segment));
        row.appendChild(createCell(record.symbol));
        row.appendChild(createCell(parseNum(record.holding_quantity)));
        row.appendChild(createCell(priceConvert(record.avg_price)));
        row.appendChild(createCell(priceConvert(record.holding_amount)));

        row.appendChild(
            createCell(
                priceConvert(record.close_price),
                pnl < 0 ? ["text-danger"] : ["text-success"]
            )
        );

        row.appendChild(
            createCell(
                priceConvert(record.close_amount),
                pnl < 0 ? ["text-danger"] : ["text-success"]
            )
        );

        row.appendChild(
            createCell(
                `${priceConvert(pnl)} (${pnl < 0 ? "+" : ""}${parseNum(
                    (pnl * 100) / record.holding_amount
                )}%)`,
                pnl < 0 ? ["text-danger"] : ["text-success"]
            )
        );
        // Append the row to the table body
        tableBody.appendChild(row);
    });
}

function updateLatestData(data) {
    const { close, holding } = data;
    const pnl = close - holding;

    const elem = document.getElementById("FinancialSummary");
    elem.innerHTML = `
        <!-- All Your Assets Worth -->
        <div class="col-sm-12 col-md-4 mb-3 mb-md-0">
            <div class="h4 ${
                pnl > 0 ? "text-success" : "text-danger"
            }">${priceConvert(close, true)}</div>
            <small class="text-secondary">All your assets worth</small>
        </div>
        <!-- Invested Amount -->
        <div class="col-sm-12 col-md-4 mb-3 mb-md-0">
            <div class="h4 text-primary">${priceConvert(holding, true)}</div>
            <small class="text-secondary">Invested amount</small>
        </div>
        <!-- Overall Returns -->
        <div class="col-sm-12 col-md-4">
            <div class="h4 ${
                pnl > 0 ? "text-success" : "text-danger"
            }">${priceConvert(pnl, true)} (${pnl > 0 ? "+" : ""}${parseNum(
        (pnl * 100) / holding
    )}%)</div>
            <small class="text-secondary">Overall Returns</small>
        </div>
    `;
}

function find_base_path() {
    if (window.location.hostname === "ptprashanttripathi.github.io") {
        return "https://raw.githubusercontent.com/ptprashanttripathi/PortfolioTracker/main/";
    } else if (
        window.location.hostname === "localhost" ||
        window.location.hostname === "127.0.0.1"
    ) {
        return "/";
    } else {
        // If the domain doesn't match the expected ones, replace the HTML with an error message
        document.body.innerHTML = `
            <div style="text-align: center; padding: 50px;">
                <h1>Error: Unsupported Domain</h1>
                <p>The application is not supported on this domain.</p>
            </div>`;
        return null; // Optionally return null or undefined since there's no valid base path
    }
}

async function main() {
    const base_path = find_base_path();

    // Fetch the data using getData
    const holdingsTrandsFilePath = `${base_path}DATA/GOLD/Holdings/HoldingsTrands_data.csv`;
    const holdingsTrands_data = await getData(holdingsTrandsFilePath);
    loadHoldingsTrandsChart(holdingsTrands_data);

    const latestData = findMaxDateRecords(holdingsTrands_data)[0];
    updateLatestData(latestData);

    // Fetch the data using getData
    const holdingsDataFilePath = `${base_path}DATA/GOLD/Holdings/Holdings_data.csv`;
    const holdingsData = await getData(holdingsDataFilePath);
    const filterdData = findMaxDateRecords(holdingsData);
    loadHoldingsDataTable(filterdData);

    // Fetch the data using getData
    const profitLossDataFilePath = `${base_path}DATA/GOLD/ProfitLoss/ProfitLoss_data.csv`;
    const profitLossData = await getData(profitLossDataFilePath);
    console.log(profitLossData[0]);
    loadProfitLossDataTable(profitLossData);
}

main();
