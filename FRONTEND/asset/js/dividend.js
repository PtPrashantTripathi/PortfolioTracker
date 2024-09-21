import "../../adminlte/plugins/jquery/jquery.min.js";
import "../../adminlte/plugins/bootstrap/js/bootstrap.bundle.min.js";
import "../../adminlte/dist/js/adminlte.min.js";
import ApexCharts from "https://cdn.jsdelivr.net/npm/apexcharts/+esm";
import {
    fetchApiData,
    priceFormat,
    loadDataTable,
    createCell,
} from "./render.js";

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
function updateDividendSummary(yearWiseData) {
    // Calculate the sum of dividend_amount
    const totalDividendAmount = yearWiseData.reduce(
        (sum, item) => sum + item.dividend_amount,
        0
    );
    document.getElementById("totalDividend").textContent =
        priceFormat(totalDividendAmount);
}
async function main() {
    const apiData = await fetchApiData();
    loadDividendDataTable(apiData.dividend_data.stock_wise);
    loadDividendChart(apiData.dividend_data.year_wise);
    updateDividendSummary(apiData.dividend_data.year_wise);
}
window.onload = main();
