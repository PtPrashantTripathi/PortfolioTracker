// Load Chart.js as Charts (UMD)
import "https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js";
// Load Chart.js Date Adapter
import "https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3.0.0/dist/chartjs-adapter-date-fns.bundle.min.js";
import getData from "./get_data.js";

// Get reference to chart holder DOM element
const chartHolder = document.getElementById("myChart").getContext("2d");

function createChart(data) {
    // Initial a new Chart with the fetched data and provided options
    return new Chart(chartHolder, {
        type: "line",
        data: {
            labels: data.map((d) => d.date),
            datasets: [
                {
                    label: "Market Value",
                    data: data.map((d) => d.close),
                    borderColor: "rgba(75, 192, 192, 1)",
                    fill: false,
                    tension: 0.1,
                    pointRadius: 0, // Remove points
                    pointHoverRadius: 0, // Disable point hover
                },
                {
                    label: "Investment",
                    data: data.map((d) => d.holding),
                    borderColor: "rgba(153, 102, 255, 1)",
                    fill: false,
                    tension: 0.1,
                    pointRadius: 0, // Remove points
                    pointHoverRadius: 0, // Disable point hover
                },
            ],
        },
        options: {
            responsive: true,
            scales: {
                x: {
                    type: "time",
                    time: {
                        unit: "day",
                        tooltipFormat: "yyyy-MM-dd",
                    },
                    title: {
                        display: true,
                        text: "Date",
                    },
                },
                y: {
                    title: {
                        display: true,
                        text: "Value",
                    },
                },
            },
        },
    });
}

function aggregateData(data, keyFunc) {
    return data.reduce((acc, current) => {
        let key = keyFunc(current.date);
        if (!acc[key] || current.date > acc[key].date) {
            acc[key] = current;
        }
        return acc;
    }, {});
}

// Asynchronous function to fetch and render the chart
async function main() {
    // Fetch the data using getData
    const filePath = "../DATA/GOLD/HoldingsTrands/HoldingsTrands_data.csv";
    const raw_data = await getData(filePath);

    // map data
    const data = raw_data.map((item) => ({
        date: new Date(item.date),
        holding: parseFloat(item.holding),
        close: parseFloat(item.close),
    }));
    let chart = createChart(data);

    // Button Handlers for Aggregation
    document.getElementById("interval").addEventListener("change", (event) => {
        const interval = event.target.value;
        let updatedData = [];
        switch (interval) {
            case "yearly":
                updatedData = Object.values(
                    aggregateData(data, (date) => date.getFullYear())
                );
                break;
            case "monthly":
                updatedData = Object.values(
                    aggregateData(data, (date) => date.getYearMonth())
                );
                break;
            case "weekly":
                updatedData = Object.values(
                    aggregateData(data, (date) => date.getYearWeek())
                );
                break;
            default:
                updatedData = data;
        }
        chart.destroy();
        chart = createChart(updatedData);
    });
}

Date.prototype.getYearMonth = function () {
    const year = this.getFullYear();
    const month = (this.getMonth() + 1).toString().padStart(2, "0"); // Months are zero-indexed, so we add 1
    return `${year}-${month}`;
};

Date.prototype.getYearWeek = function () {
    const dayMilliseconds = 86400000;

    // Start of the year (January 1st)
    const oneJan = new Date(this.getFullYear(), 0, 1);

    // Find the first Monday of the year
    const oneJanDay = oneJan.getDay() || 7; // Adjust for Sunday being day 0
    const firstMondayTime =
        oneJan.getTime() + (8 - oneJanDay) * dayMilliseconds;
    const firstMonday = new Date(firstMondayTime);

    // Current date at midnight
    const currentDate = new Date(
        this.getFullYear(),
        this.getMonth(),
        this.getDate()
    );
    const daysFromFirstMonday = Math.round(
        (currentDate - firstMonday) / dayMilliseconds
    );

    // Calculate the week number
    let weekNum =
        daysFromFirstMonday >= 0 && daysFromFirstMonday < 364
            ? Math.ceil((daysFromFirstMonday + 1) / 7)
            : 52;

    // Handle cases where the first week is part of the previous year
    if (daysFromFirstMonday < 0) {
        weekNum = 52;
    }

    // Ensure the week number is zero-padded
    const formattedWeekNum = weekNum.toString().padStart(2, "0");

    return `${this.getFullYear()}-${formattedWeekNum}`;
};

// Call the function to render the chart
main();
