import {
    fetchApiData,
    priceFormat,
    parseNum,
    renderSummary,
    createCell,
    loadDataTable,
    calcDays,
    updated_header_footer,
} from "./render.js";

// Update the P&L data summary section
function updateProfitLossDataSummary(data) {
    // Function to calculate profit/loss summary
    const investedValue = data.reduce(
        (sum, record) => sum + record.open_amount,
        0
    );
    const soldValue = data.reduce(
        (sum, record) => sum + record.close_amount,
        0
    );
    const pnlValue = data.reduce((sum, record) => sum + record.pnl_amount, 0);

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

// Process the raw data received from the Python DataFrame
function processProfitLossData(data) {
    // Parse dates and calculate days for each record
    data.forEach((record) => {
        record.open_datetime = new Date(record.open_datetime);
        record.close_datetime = new Date(record.close_datetime);
        record.days = Math.floor(
            (record.close_datetime - record.open_datetime) /
                (1000 * 60 * 60 * 24)
        );

        if (record.segment === "FO") {
            record.close_price = record.pnl_amount / record.quantity;
            record.close_amount = record.pnl_amount;
            record.open_price = 0;
            record.open_amount = 0;
        }
    });

    // Group the data by segment, exchange, and symbol
    const groupedData = Object.groupBy(
        data,
        (item) => `${item.segment}-${item.exchange}-${item.symbol}`
    );

    // Transform the grouped data into the desired format
    const profitLossData = Object.keys(groupedData).map((key) => {
        const group = groupedData[key];
        const totalQuantity = group.reduce(
            (sum, item) => sum + item.quantity,
            0
        );
        const totalOpenAmount = group.reduce(
            (sum, item) => sum + item.open_amount,
            0
        );
        const totalCloseAmount = group.reduce(
            (sum, item) => sum + item.close_amount,
            0
        );

        return {
            segment: group[0].segment,
            exchange: group[0].exchange,
            symbol: group[0].symbol,

            min_datetime: new Date(
                Math.min(...group.map((item) => new Date(item.open_datetime)))
            ),
            max_datetime: new Date(
                Math.max(...group.map((item) => new Date(item.close_datetime)))
            ),
            quantity: totalQuantity,
            open_amount: totalOpenAmount,
            close_amount: totalCloseAmount,
            avg_price: totalOpenAmount / totalQuantity,
            sell_price: totalCloseAmount / totalQuantity,
            pnl: group.reduce((sum, item) => sum + item.pnl_amount, 0),
            brokerage: group.reduce((sum, item) => sum + item.brokerage, 0),
            history: group,
        };
    });

    return profitLossData;
}

// Call this function to load the table once you have processed data
function loadProfitLossDataTable(data) {
    const processedData = processProfitLossData(data);

    const headers = [
        "Stock Name",
        "Qty.",
        "Avg Price",
        "Sell Price",
        "Realized PNL",
        "PNL Percentage",
        "<a href='#footnote'>Brokerage<b>*</b></a>",
        "Holding Days",
    ];

    const cellData = processedData.map((record) => {
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
            createCell(priceFormat(record.brokerage)),
            createCell(calcDays(record.min_datetime, record.max_datetime)),
        ];
    });
    loadDataTable("ProfitLossTable", headers, cellData);
}

async function main() {
    const { data: profit_loss_data, load_timestamp } = await fetchApiData(
        "profit_loss_data.json"
    );
    loadProfitLossDataTable(profit_loss_data);
    updateProfitLossDataSummary(profit_loss_data);
    updated_header_footer(load_timestamp);
}
window.onload = main();
