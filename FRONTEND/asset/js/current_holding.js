import {
    fetchApiData,
    priceFormat,
    parseNum,
    renderSummary,
    createCell,
    calcDays,
    loadDataTable,
    updated_header_footer,
} from "./render.js";

// Load current holding table
function loadCurrentHoldingDataTable(data) {
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

    const currentHoldingData = Object.entries(
        Object.groupBy(
            data,
            (item) => `${item.segment}-${item.exchange}-${item.symbol}`,
        ),
    ).map(([key, group]) => {
        const total_quantity = group.reduce(
            (sum, item) => sum + item.quantity,
            0,
        );
        const total_amount = group.reduce((sum, item) => sum + item.amount, 0);
        const close_price = group[0].close_price;
        const close_amount = close_price * total_quantity;

        return {
            key,
            scrip_name: group[0].scrip_name,
            symbol: group[0].symbol,
            exchange: group[0].exchange,
            segment: group[0].segment,
            total_quantity,
            total_amount, // Ensure total_amount is included
            avg_price: total_quantity ? total_amount / total_quantity : 0, // Avoid division by zero
            close_price,
            close_amount,
            pnl_amount: close_amount - total_amount,
            min_datetime: new Date(
                Math.min(...group.map((item) => new Date(item.datetime))),
            ),
            history: group,
        };
    });

    currentHoldingData.sort((a, b) => a.key.localeCompare(b.key));

    const cellData = currentHoldingData.map((record) => {
        const pnlClass = record.pnl_amount < 0 ? "text-danger" : "text-success";
        return [
            createCell(
                `${
                    record.segment === "EQ" ? record.symbol : record.scrip_name
                } (${record.segment})`,
            ),
            createCell(parseNum(record.total_quantity)),
            createCell(priceFormat(record.avg_price)),
            createCell(priceFormat(record.total_amount)),
            createCell(priceFormat(record.close_price), [pnlClass]),
            createCell(priceFormat(record.close_amount), [pnlClass]),
            createCell(priceFormat(record.pnl_amount), [pnlClass]),
            createCell(
                `${record.pnl_amount >= 0 ? "+" : ""}${parseNum(
                    (record.pnl_amount * 100) / record.total_amount,
                )}%`,
                [pnlClass],
            ),
            createCell(calcDays(record.min_datetime)),
        ];
    });
    loadDataTable("CurrentHoldingTable", headers, cellData);
}

// Update the financial summary section
function updateFinancialSummary(data) {
    // Function to calculate profit/loss summary
    const investedValue = data.reduce((sum, record) => sum + record.amount, 0);
    const currentValue = data.reduce(
        (sum, record) => sum + record.close_amount,
        0,
    );
    const pnlValue = data.reduce((sum, record) => sum + record.pnl_amount, 0);

    const pnlClass = pnlValue > 0 ? "bg-success" : "bg-danger";
    const pnlIcon = pnlValue > 0 ? "▲" : "▼";
    const summaryItems = [
        {
            value: priceFormat(currentValue),
            label: "Total Assets Worth",
            colorClass: "bg-info",
            iconClass: "fas fa-coins",
            href: "current_holding.html#CurrentHoldingTable",
        },
        {
            value: priceFormat(investedValue),
            label: "Total Investment",
            colorClass: "bg-warning",
            iconClass: "fas fa-cart-shopping",
            href: "current_holding.html#CurrentHoldingTable",
        },
        {
            value: priceFormat(pnlValue),
            label: "Total P&L",
            colorClass: pnlClass,
            iconClass: "fas fa-chart-pie",
            href: "current_holding.html#CurrentHoldingTable",
        },
        {
            value: `${pnlIcon} ${parseNum((pnlValue * 100) / investedValue)}%`,
            label: "Overall Return",
            colorClass: pnlClass,
            iconClass: "fas fa-chart-line",
            href: "current_holding.html#CurrentHoldingTable",
        },
    ];
    renderSummary("FinancialSummary", summaryItems);
}

async function main() {
    const { data, load_timestamp } = await fetchApiData(
        "current_holding_data.json",
    );
    loadCurrentHoldingDataTable(data);
    updateFinancialSummary(data);
    updated_header_footer(load_timestamp);
}
window.onload = main();
