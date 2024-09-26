import "../../adminlte/plugins/jquery/jquery.min.js";
import "../../adminlte/plugins/bootstrap/js/bootstrap.bundle.min.js";
import "../../adminlte/dist/js/adminlte.min.js";
import {
    fetchApiData,
    loadCurrentHoldingsDataTable,
    priceFormat,
    parseNum,
    renderSummary,
} from "./render.js";

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
            href: "current_holdings.html#CurrentHoldingsTable",
        },
        {
            value: priceFormat(investedValue),
            label: "Total Investment",
            colorClass: "bg-warning",
            iconClass: "fas fa-cart-shopping",
            href: "current_holdings.html#CurrentHoldingsTable",
        },
        {
            value: priceFormat(pnlValue),
            label: "Total P&L",
            colorClass: pnlClass,
            iconClass: "fas fa-chart-pie",
            href: "current_holdings.html#CurrentHoldingsTable",
        },
        {
            value: `${pnlIcon} ${parseNum((pnlValue * 100) / investedValue)}%`,
            label: "Overall Return",
            colorClass: pnlClass,
            iconClass: "fas fa-chart-line",
            href: "current_holdings.html#CurrentHoldingsTable",
        },
    ];
    renderSummary("FinancialSummary", summaryItems);
}

async function main() {
    const apiData = await fetchApiData();
    loadCurrentHoldingsDataTable(apiData.current_holding_data);
    updateFinancialSummary(
        apiData.financial_summary.invested_value,
        apiData.financial_summary.current_value,
        apiData.financial_summary.pnl_value
    );
}
window.onload = main();
