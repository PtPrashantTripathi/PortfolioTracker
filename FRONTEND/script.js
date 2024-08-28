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

// Function to get data based on file type (CSV or JSON)
async function getData(filePath) {
    try {
        const response = await fetch(filePath);

        // Check if the file exists and was fetched successfully
        if (!response.ok) {
            throw new Error(`Failed to fetch the file: ${response.statusText}`);
        }

        // Handle CSV file
        if (filePath.endsWith(".csv")) {
            const csvData = await response.text();
            const parsedData = parseCSV(csvData); // Parse the CSV data
            return parsedData;

            // Handle JSON file
        } else if (filePath.endsWith(".json")) {
            const jsonData = await response.json();
            return jsonData;

            // Handle unknown file type
        } else {
            throw new Error(
                "Unknown data type. Please provide a valid CSV or JSON file."
            );
        }
    } catch (error) {
        console.error("Error fetching or processing the data:", error);
        throw error;
    }
}

async function loadHoldingsTrandsChart(data) {
    const options = {
        series: [
            {
                name: "Investment",
                data: data.map((d) => ({
                    x: new Date(d.date),
                    y: d.holding,
                })),
            },
            {
                name: "Market Value",
                data: data.map((d) => ({
                    x: new Date(d.date),
                    y: d.close,
                })),
            },
        ],
        // responsive: true,
        chart: {
            type: "area",
            height: "100%", // allows flexibility for responsive designs
            width: "100%",

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
            //   floating: false,
            labels: {
                formatter: (a) => priceFormat(a, true),
                style: {
                    colors: "#8e8da4",
                },
            },
            axisBorder: {
                show: true,
            },
            axisTicks: {
                show: true,
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
                formatter: (a) => priceFormat(a),
            },
        },
        legend: {
            show: false, // Hide legend
        },
        grid: {
            show: false, // Hides both x and y axis grid lines
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

function priceFormat(value, short = false) {
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

function calcDays(date1, date2 = null) {
    // Parse the first date string into a Date object
    const firstDate = new Date(date1);

    // If the second date is provided, use it; otherwise, use the current date
    const secondDate = date2 ? new Date(date2) : new Date();

    // Calculate the difference in milliseconds
    const differenceInMilliseconds = secondDate - firstDate;

    // Convert the difference from milliseconds to days and use Math.abs() to ensure positive days
    const differenceInDays = Math.abs(
        Math.floor(differenceInMilliseconds / (1000 * 60 * 60 * 24))
    );

    return differenceInDays;
}

async function loadProfitLossDataTable(data) {
    const table = document.getElementById("ProfitLossTable");
    // Create and append the table header
    table.innerHTML = `
    <thead>
      <tr>
        <th>Stock Name</th>
        <th>Qty.</th>
        <th>Buy Price</th>
        <th>Sell Price</th>
        <th>PNL</th>
        <th>Percentage</th>
        <th>Days</th>
      </tr>
    </thead>
  `;

    const tableBody = document.createElement("tbody");

    data.forEach((record) => {
        const row = document.createElement("tr");
        const pnl_flag = record.pnl < 0;
        // Create and append cells
        row.appendChild(createCell(`${record.symbol} (${record.segment})`));
        row.appendChild(createCell(parseNum(record.quantity)));
        row.appendChild(createCell(priceFormat(record.avg_price)));
        row.appendChild(createCell(priceFormat(record.sell_price)));
        row.appendChild(
            createCell(
                priceFormat(record.pnl),
                pnl_flag ? ["text-danger"] : ["text-success"]
            )
        );

        row.appendChild(
            createCell(
                `${pnl_flag ? "" : "+"}${parseNum(
                    (record.pnl * 100) / (record.avg_price * record.quantity)
                )}%`,
                pnl_flag ? ["text-danger"] : ["text-success"]
            )
        );
        row.appendChild(createCell(record.days));
        // Append the row to the table body
        tableBody.appendChild(row);
    });

    // Append the table body to the table
    table.appendChild(tableBody);
}

async function loadCurrentHoldingsDataTable(data) {
    const table = document.getElementById("CurrentHoldingsTable"); // <table class="table m-0" id="CurrentHoldingsTable"></table>

    // Create and append the table header
    table.innerHTML = `
    <thead>
      <tr>
        <th>Stock Name</th>
        <th>Qty.</th>
        <th>Avg Price</th>
        <th>Invested Value</th>
        <th>CMP Price</th>
        <th>Current Value</th>
        <th>Unrealized PNL</th>
        <th>PNL Percantage</th>
        <th>Days</th>
      </tr>
    </thead>
  `;

    const tableBody = document.createElement("tbody");
    data.forEach((record) => {
        const row = document.createElement("tr");
        const pnlClass = record.pnl_amount < 0 ? "text-danger" : "text-success";

        const cells = [
            createCell(
                `${
                    record.segment == "EQ" ? record.symbol : record.scrip_name
                } (${record.segment})`
            ),
            createCell(parseNum(record.total_quantity)),
            createCell(priceFormat(record.avg_price)),
            createCell(priceFormat(record.total_amount)),
            createCell(priceFormat(record.close_price), [pnlClass]),
            createCell(priceFormat(record.close_amount), [pnlClass]),
            createCell(priceFormat(record.pnl_amount), [pnlClass]),
            createCell(
                `${record.pnl_amount >= 0 ? "+" : ""}${parseNum(
                    (record.pnl_amount * 100) / record.total_amount
                )}%`,
                [pnlClass]
            ),
            createCell(calcDays(record.min_datetime)),
        ];

        // Append all cells to the row
        cells.forEach((cell) => row.appendChild(cell));

        // Append the row to the table body
        tableBody.appendChild(row);
    });

    // Append the table body to the table
    table.appendChild(tableBody);
}

// Helper function to create a cell with text content and optional classes
function createCell(text, classes = []) {
    const cell = document.createElement("td");
    cell.innerHTML = text;
    classes.forEach((cls) => cell.classList.add(cls));
    return cell;
}

function updateFinancialSummary(data) {
    const { current_value, invested_value, overall_pnl } = data;
    const pnlClass = overall_pnl > 0 ? "bg-success" : "bg-danger";
    const pnlIcon = overall_pnl > 0 ? "▲" : "▼";

    const summaryItems = [
        {
            value: priceFormat(current_value),
            label: "Total Assets Worth",
            colorClass: "bg-info",
            iconClass: "fas fa-coins",
            link: "#",
        },
        {
            value: priceFormat(invested_value),
            label: "Total Investment",
            colorClass: "bg-warning",
            iconClass: "fas fa-cart-shopping",
            link: "#",
        },
        {
            value: priceFormat(overall_pnl),
            label: "Total P&L",
            colorClass: pnlClass,
            iconClass: "fas fa-chart-pie",
            link: "#",
        },
        {
            value: `${pnlIcon}${parseNum(
                (overall_pnl * 100) / invested_value
            )}%`,
            label: "Overall Return",
            colorClass: pnlClass,
            iconClass: "fas fa-chart-line",
            link: "#",
        },
    ];

    const elem = document.getElementById("FinancialSummary");
    elem.innerHTML = summaryItems
        .map(
            (item) => `
            <div class="col-lg-3 col-6">
            <div class="small-box ${item.colorClass}">
                <div class="inner">
                <h4>${item.value}</h4>
                <h6>${item.label}</h6>
                </div>
                <div class="icon">
                <i class="${item.iconClass}"></i>
                </div>
                <a href="${item.link}" class="small-box-footer">
                More info <i class="fas fa-arrow-circle-right"></i>
                </a>
            </div>
            </div>
            `
        )
        .join("");
}

function updateProfitLossDataSummary(data) {
    const { sold, invested, pnl } = data;

    const pnlPercentage = (pnl * 100) / invested;

    const summaryItems = [
        {
            value: priceFormat(sold),
            label: "TOTAL ASSETS TURNOVER",
            iconColorClass: "bg-info",
            numberClass: pnl > 0 ? "text-success" : "text-danger",
            iconClass: "fas fa-solid fa-coins",
            link: "#",
        },
        {
            value: priceFormat(invested),
            label: "TOTAL INVESTED",
            iconColorClass: "bg-warning",
            numberClass: "text-info",
            iconClass: "fas fa-piggy-bank",
            link: "#",
        },
        {
            value: `${priceFormat(pnl)} (${pnl > 0 ? "+" : ""}${parseNum(
                pnlPercentage
            )}%)`,
            label: "OVERALL REALIZED PNL",
            iconColorClass: pnl > 0 ? "bg-success" : "bg-danger",
            numberClass: pnl > 0 ? "text-success" : "text-danger",
            iconClass: "fas fa-percent",
            link: "#",
        },
    ];

    const elem = document.getElementById("PNLSummary");
    elem.innerHTML = summaryItems
        .map(
            (item) => `
            <div class="col-lg-4 col-12 p-0">
            <div class="info-box m-0">
            <span class="info-box-icon ${item.iconColorClass} elevation-1"><i class="${item.iconClass}"></i></span>
            <div class="info-box-content">
                <span class="info-box-text">${item.label}</span>
                <span class="info-box-number ${item.numberClass}">${item.value}</span>
            </div>
            </div>
            </div>
            `
        )
        .join("");
}

function find_base_path() {
    if (window.location.hostname === "ptprashanttripathi.github.io") {
        return "https://raw.githubusercontent.com/PtPrashantTripathi/PortfolioTracker/main";
    } else {
        return "..";
    }
}

async function main() {
    const base_path = find_base_path();

    // Fetch the data using getData
    const holdingsTrandsFilePath = `${base_path}/DATA/API/HoldingsTrands_data.json`;
    const holdingsTrands_data = await getData(holdingsTrandsFilePath);
    loadHoldingsTrandsChart(holdingsTrands_data);

    // Fetch the data using getData
    const currentHoldingsDataFilePath = `${base_path}/DATA/API/CurrentHoldings_data.json`;
    const currentHoldingsData = await getData(currentHoldingsDataFilePath);
    loadCurrentHoldingsDataTable(currentHoldingsData.data);
    updateFinancialSummary(currentHoldingsData);

    // Fetch the data using getData
    const profitLossDataFilePath = `${base_path}/DATA/API/ProfitLoss_data.json`;
    const profitLossData = await getData(profitLossDataFilePath);
    loadProfitLossDataTable(profitLossData.data);
    updateProfitLossDataSummary(profitLossData);
}

main();
