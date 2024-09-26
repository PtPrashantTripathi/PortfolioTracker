import "../../adminlte/plugins/jquery/jquery.min.js";
import "../../adminlte/plugins/bootstrap/js/bootstrap.bundle.min.js";
import "../../adminlte/dist/js/adminlte.min.js";

import {
    fetchApiData,
    priceFormat,
    parseNum,
    renderSummary,
    createCell,
    loadDataTable,
} from "./render.js";

// Update the P&L data summary section
function updateProfitLossDataSummary(investedValue, soldValue, pnlValue) {
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
async function main() {
    const apiData = await fetchApiData();
    loadProfitLossDataTable(apiData.profit_loss_data);
    updateProfitLossDataSummary(
        apiData.profitloss_summary.invested_value,
        apiData.profitloss_summary.sold_value,
        apiData.profitloss_summary.pnl_value
    );
}
window.onload = main();
