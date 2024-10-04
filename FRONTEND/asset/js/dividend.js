import ApexCharts from "https://cdn.jsdelivr.net/npm/apexcharts/+esm";
import {
    fetchApiData,
    priceFormat,
    loadDataTable,
    createCell,
} from "./render.js";

// Load ApexCharts chart
function loadDividendChart(data) {
    const yearWiseData = Object.groupBy(data, (item) => item.financial_year);
    const financialYears = Object.keys(yearWiseData).sort((a, b) =>
        a.localeCompare(b)
    );
    const dividendAmounts = financialYears.map((financial_year) =>
        yearWiseData[financial_year].reduce(
            (sum, item) => sum + item.dividend_amount,
            0
        )
    );

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
            categories: financialYears,
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

function loadDividendDataTable(data) {
    // Extract unique fy and sort them in ascending order
    const uniqueFY = Array.from(
        new Set(data.map((item) => item.financial_year))
    ).sort((a, b) => a.localeCompare(b));

    const headers = ["Stock Name", ...uniqueFY, "Total"];

    // Group data by 'symbol' and 'segment'
    const stockWiseData = Object.entries(
        Object.groupBy(data, (item) => `${item.symbol} (${item.segment})`)
    ).map(([symbol, symbolGroup]) => ({
        symbol,
        dividend_amount: symbolGroup.reduce(
            (sum, item) => sum + item.dividend_amount,
            0
        ),
        data: Object.entries(
            Object.groupBy(symbolGroup, (item) => item.financial_year)
        ).map(([financial_year, fyGroup]) => ({
            financial_year,
            dividend_amount: fyGroup.reduce(
                (sum, item) => sum + item.dividend_amount,
                0
            ),
        })),
    }));

    stockWiseData.sort((a, b) => a.symbol.localeCompare(b.symbol));

    // Constructing objects with symbols and dividend amounts for each year
    const cellData = stockWiseData.map((record) => {
        const result = [createCell(record.symbol)];

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

function updateDividendSummary(data) {
    // Calculate the sum of dividend_amount
    const totalDividendAmount = data.reduce(
        (sum, item) => sum + item.dividend_amount,
        0
    );
    document.getElementById("totalDividend").textContent =
        priceFormat(totalDividendAmount);
}

async function main() {
    const { data, load_timestamp } = await fetchApiData("dividend_data.json");
    loadDividendDataTable(data);
    loadDividendChart(data);
    updateDividendSummary(data);
}

window.onload = main;
