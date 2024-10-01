import "../../adminlte/js/jquery/jquery.js";
import "../../adminlte/js/bootstrap/js/bootstrap.bundle.js";
import "../../adminlte/js/adminlte.js";
import {
    fetchApiData,
    priceFormat,
    parseNum,
    renderSummary,
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
    loadDataTable("CurrentHoldingTable", headers, cellData);
}

// Update the financial summary section
function updateFinancialSummary(investedValue, currentValue, pnlValue) {
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
    const apiData = await fetchApiData();
    loadCurrentHoldingDataTable(apiData.current_holding_data);
    updateFinancialSummary(
        apiData.financial_summary.invested_value,
        apiData.financial_summary.current_value,
        apiData.financial_summary.pnl_value
    );
}
window.onload = main();
