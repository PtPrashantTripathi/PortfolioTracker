// Utility function for number formatting
export function priceFormat(value, short = false) {
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
export function parseNum(value) {
    return parseFloat(value) === parseInt(value)
        ? parseInt(value)
        : parseFloat(parseFloat(value).toFixed(2));
}

// Utility function to calculate days between dates
export function calcDays(date1, date2 = new Date()) {
    const firstDate = new Date(date1);
    const secondDate = new Date(date2);
    const differenceInDays = Math.abs(
        Math.floor((secondDate - firstDate) / (1000 * 60 * 60 * 24))
    );
    return differenceInDays;
}

// Creates a table cell with optional classes
export function createCell(content, classes = []) {
    const cell = document.createElement("td");
    cell.innerHTML = content;
    classes.forEach((cls) => cell.classList.add(cls));
    return cell;
}

//export function to load data into a table
export function loadDataTable(tableId, headers, cellData) {
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
export function loadCurrentHoldingsDataTable(data) {
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

// Render summary sections
export function renderSummary(elementId, summaryItems) {
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
                    <a href="${item.href}" class="small-box-footer">
                        More info <i class="fas fa-arrow-circle-right"></i>
                    </a>
                </div>
            </div>
        `
        )
        .join("");
}

export async function fetchApiData() {
    try {
        const apiPath = "../DATA/API/API_data.json";
        const apiResponse = await fetch(apiPath);

        // Validate HTTP response status
        if (!apiResponse.ok) {
            throw new Error(`HTTP error! status: ${apiResponse.status}`);
        }

        // Parse and return the JSON data
        const apiData = await apiResponse.json();
        return apiData;
    } catch (error) {
        console.error("Error fetching API data:");
        throw error; // Re-throw the error for further handling
    }
}
