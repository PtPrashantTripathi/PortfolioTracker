import ApexCharts from "https://cdn.jsdelivr.net/npm/apexcharts/+esm";
import {
    fetchApiData,
    priceFormat,
    parseNum,
    renderSummary,
} from "./render.js";

// Load ApexCharts chart
function loadHoldingTrandsChart(data) {
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
        document.getElementById("holdingTrandsChart"),
        options
    );
    chart.render();
    return chart;
}

// Update the financial summary section
function updateFinancialSummary(data) {
    // Function to calculate profit/loss summary
    const investedValue = data.reduce(
        (sum, record) => sum + record.total_amount,
        0
    );
    const currentValue = data.reduce(
        (sum, record) => sum + record.close_amount,
        0
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
    const { data: holding_trands_data, load_timestamp } = await fetchApiData(
        "holding_trands_data.json"
    );
    loadHoldingTrandsChart(holding_trands_data);

    const { data } = await fetchApiData("current_holding_data.json");
    updateFinancialSummary(data);
}
window.onload = main();
