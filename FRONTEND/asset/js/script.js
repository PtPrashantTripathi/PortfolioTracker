// Utility function for number formatting
function priceFormat(value, short = false) {
    const sign = value >= 0 ? "" : "-";
    value = parseFloat(value);
    if (short) {
        value = Math.abs(value);
        if (value >= 1e7) return `${sign}₹${(value / 1e7).toFixed(2)}Cr`;
        if (value >= 1e5) return `${sign}₹${(value / 1e5).toFixed(2)}L`;
        if (value >= 1e3) return `${sign}₹${(value / 1e3).toFixed(2)}K`;
    }
    return value.toLocaleString("en-IN", {
        maximumFractionDigits: 2,
        style: "currency",
        currency: "INR",
    });
}

// Utility function for parsing numbers
function parseNum(value) {
    return parseFloat(value) === parseInt(value)
        ? parseInt(value)
        : parseFloat(parseFloat(value).toFixed(2));
}

// Utility function to calculate days between dates
function calcDays(date1, date2 = new Date()) {
    const firstDate = new Date(date1);
    const secondDate = new Date(date2);
    const differenceInDays = Math.abs(
        Math.floor((secondDate - firstDate) / (1000 * 60 * 60 * 24))
    );
    return differenceInDays;
}

// Creates a table cell with optional classes
function createCell(content, classes = []) {
    const cell = document.createElement("td");
    cell.innerHTML = content;
    classes.forEach((cls) => cell.classList.add(cls));
    return cell;
}

// Function to load data into a table
function loadDataTable(tableId, headers, cellData) {
    const table = document.getElementById(tableId);
    const tableHead = document.createElement("thead");

    const row = document.createElement("tr");
    headers.forEach((cell) => row.appendChild(createCell(cell)));
    tableHead.appendChild(row);
    table.appendChild(tableHead);

    const tableBody = document.createElement("tbody");
    cellData.forEach((record) => {
        const row = document.createElement("tr");
        record.forEach((cell) => row.appendChild(cell));
        tableBody.appendChild(row);
    });
    table.appendChild(tableBody);
}

// Load current holdings table
function loadCurrentHoldingsDataTable(data) {
    const headers = [
        "Stock Name",
        "Qty.",
        "Avg Price",
        "Invested Value",
        "CMP Price",
        "Current Value",
        "Unrealized PNL",
        "PNL Percentage",
        "Holding Days",
    ];

    const cellData = data.map((record) => {
        const pnlClass = record.pnl_amount < 0 ? "text-danger" : "text-success";
        return [
            createCell(
                `${
                    record.segment === "EQ" ? record.symbol : record.scrip_name
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
    });
    loadDataTable("CurrentHoldingsTable", headers, cellData);
}

function loadDividendDataTable(stockWiseData) {
    // Extract unique fy and sort them in ascending order
    const uniqueFY = Array.from(
        new Set(
            stockWiseData.flatMap((item) =>
                item.data.map((subItem) => subItem.financial_year)
            )
        )
    ).sort((a, b) => a.localeCompare(b));

    const headers = ["Stock Name", ...uniqueFY, "Total"];

    // Constructing objects with symbols and dividend amounts for each year
    const cellData = stockWiseData.map((record) => {
        const result = [createCell(`${record.symbol} (${record.segment})`)];

        // Add dividend amounts for each unique year
        uniqueFY.forEach((fy) => {
            const financialYearData = record.data.find(
                (subRecord) => subRecord.financial_year === fy
            );
            // Find data for the specific year
            result.push(
                financialYearData
                    ? createCell(
                          priceFormat(financialYearData.dividend_amount),
                          ["text-success"]
                      )
                    : createCell("-", ["text-info"])
            );
        });

        // Add the total dividend amount for the symbol
        result.push(
            createCell(priceFormat(record.dividend_amount), ["text-success"])
        );
        return result;
    });

    // Load the data into the DataTable
    loadDataTable("DividendTable", headers, cellData);
}

// Load profit/loss table
function loadProfitLossDataTable(data) {
    const headers = [
        "Stock Name",
        "Qty.",
        "Avg Price",
        "Sell Price",
        "Realized PNL",
        "PNL Percentage",
        "Holding Days",
    ];
    const cellData = data.map((record) => {
        const pnlFlag = record.pnl < 0;
        return [
            createCell(`${record.symbol} (${record.segment})`),
            createCell(parseNum(record.quantity)),
            createCell(priceFormat(record.avg_price)),
            createCell(priceFormat(record.sell_price)),
            createCell(
                priceFormat(record.pnl),
                pnlFlag ? ["text-danger"] : ["text-success"]
            ),
            createCell(
                `${pnlFlag ? "" : "+"}${parseNum(
                    (record.pnl * 100) / (record.avg_price * record.quantity)
                )}%`,
                pnlFlag ? ["text-danger"] : ["text-success"]
            ),
            createCell(record.days),
        ];
    });
    loadDataTable("ProfitLossTable", headers, cellData);
}

// Render summary sections
function renderSummary(elementId, summaryItems) {
    const elem = document.getElementById(elementId);
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
                    <a href="#" class="small-box-footer">
                        More info <i class="fas fa-arrow-circle-right"></i>
                    </a>
                </div>
            </div>
        `
        )
        .join("");
}

// Update the financial summary section
function updateFinancialSummary(investedValue, currentValue, pnlValue) {
    console.log({
        investedValue: investedValue,
        currentValue: currentValue,
        pnlValue: pnlValue,
    });
    const pnlClass = pnlValue > 0 ? "bg-success" : "bg-danger";
    const pnlIcon = pnlValue > 0 ? "▲" : "▼";
    const summaryItems = [
        {
            value: priceFormat(currentValue),
            label: "Total Assets Worth",
            colorClass: "bg-info",
            iconClass: "fas fa-coins",
        },
        {
            value: priceFormat(investedValue),
            label: "Total Investment",
            colorClass: "bg-warning",
            iconClass: "fas fa-cart-shopping",
        },
        {
            value: priceFormat(pnlValue),
            label: "Total P&L",
            colorClass: pnlClass,
            iconClass: "fas fa-chart-pie",
        },
        {
            value: `${pnlIcon} ${parseNum((pnlValue * 100) / investedValue)}%`,
            label: "Overall Return",
            colorClass: pnlClass,
            iconClass: "fas fa-chart-line",
        },
    ];
    renderSummary("FinancialSummary", summaryItems);
}

// Update the P&L data summary section
function updateProfitLossDataSummary(investedValue, soldValue, pnlValue) {
    console.log({
        investedValue: investedValue,
        soldValue: soldValue,
        pnlValue: pnlValue,
    });

    const pnlClass = pnlValue > 0 ? "bg-success" : "bg-danger";
    const pnlIcon = pnlValue > 0 ? "▲" : "▼";
    const summaryItems = [
        {
            value: priceFormat(soldValue),
            label: "Total Turnover",
            colorClass: "bg-info",
            iconClass: "fas fa-solid fa-coins",
        },
        {
            value: priceFormat(investedValue),
            label: "Total Invested",
            colorClass: "bg-warning",
            iconClass: "fas fa-piggy-bank",
        },
        {
            value: priceFormat(pnlValue),
            label: "Total P&L",
            colorClass: pnlClass,
            iconClass: "fas fa-chart-pie",
        },
        {
            value: `${pnlIcon} ${parseNum((pnlValue * 100) / investedValue)}%`,
            label: "Overall Return",
            colorClass: pnlClass,
            iconClass: "fas fa-percent",
        },
    ];
    renderSummary("PNLSummary", summaryItems);
}

// Load ApexCharts chart
function loadHoldingsTrandsChart(data) {
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
        chart: {
            type: "area",
            height: "100%",
            width: "100%",
            toolbar: {
                show: true,
            },
            zoom: {
                enabled: true,
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
            show: false,
        },
        grid: {
            show: false,
        },
    };
    const chart = new ApexCharts(
        document.getElementById("holdingsTrandsChart"),
        options
    );
    chart.render();
    return chart;
}

function updateDividendSummary(yearWiseData) {
    // Calculate the sum of dividend_amount
    const totalDividendAmount = yearWiseData.reduce(
        (sum, item) => sum + item.dividend_amount,
        0
    );
    document.getElementById("totalDividend").textContent =
        priceFormat(totalDividendAmount);
}

// Load ApexCharts chart
function loadDividendChart(yearWiseData) {
    const financial_year = yearWiseData.map((item) => item.financial_year);
    const dividendAmounts = yearWiseData.map((item) => item.dividend_amount);

    const options = {
        chart: {
            type: "bar",

            height: 350,
        },
        series: [
            {
                name: "Dividend Amount",

                data: dividendAmounts,
            },
        ],
        xaxis: {
            categories: financial_year,

            title: {
                text: "Financial Year",
            },
            axisBorder: {
                show: false,
            },
            axisTicks: {
                show: false,
            },
        },
        yaxis: {
            labels: {
                formatter: (a) => priceFormat(a, false),
                style: {
                    colors: "#28a745",
                },
            },
            axisBorder: {
                show: true,
            },
            axisTicks: {
                show: true,
            },
            title: {
                text: "Dividend Amount (in Rupees)",
            },
        },
        title: {
            text: "Year-wise Dividend Amount",

            align: "center",
        },
        colors: ["#28a745"],

        plotOptions: {
            bar: {
                horizontal: false,

                columnWidth: "50%",
            },
        },
        dataLabels: {
            enabled: false,
        },
    };

    const chart = new ApexCharts(
        document.getElementById("dividendBarChart"),
        options
    );
    chart.render();
    return chart;
}

// Main function to fetch and Update latest data and UI
async function main() {
    const apiPath =
        window.location.hostname === "harshalk2022.github.io"
            ? "https://raw.githubusercontent.com/harshalk2022/PortfolioTracker/main/DATA/API/API_data.json"
            : "../DATA/API/API_data.json";

    const apiResponse = await fetch(apiPath);
    const apiData = await apiResponse.json();

    loadCurrentHoldingsDataTable(apiData.current_holding_data);
    loadProfitLossDataTable(apiData.profit_loss_data);
    loadHoldingsTrandsChart(apiData.holdings_trands_data);
    updateFinancialSummary(
        apiData.financial_summary.invested_value,
        apiData.financial_summary.current_value,
        apiData.financial_summary.pnl_value
    );
    updateProfitLossDataSummary(
        apiData.profitloss_summary.invested_value,
        apiData.profitloss_summary.sold_value,
        apiData.profitloss_summary.pnl_value
    );
    loadDividendDataTable(apiData.dividend_data.stock_wise);
    loadDividendChart(apiData.dividend_data.year_wise);
    updateDividendSummary(apiData.dividend_data.year_wise);
}

main();
