async function loadHoldingsTrandsChart() {
    try {
        // Fetch the data using getData
        const filePath = "../DATA/GOLD/HoldingsTrands/HoldingsTrands_data.csv";
        const rawData = await getData(filePath);

        // Map data to the format needed for Chart.js
        const data = {
            labels: rawData.map((d) => new Date(d.date)),
            datasets: [
                {
                    label: "Market Value",
                    data: rawData.map((d) => parseFloat(d.close)),
                    borderColor: "rgba(75, 192, 192, 1)",
                    fill: false,
                    tension: 0.1,
                    pointRadius: 0,
                    pointHoverRadius: 0,
                },
                {
                    label: "Investment",
                    data: rawData.map((d) => parseFloat(d.holding)),
                    borderColor: "rgba(153, 102, 255, 1)",
                    fill: false,
                    tension: 0.1,
                    pointRadius: 0,
                    pointHoverRadius: 0,
                },
            ],
        };
        const options = {
            responsive: true,
            scales: {
                x: {
                    type: "time",
                    time: {
                        unit: "quarter",
                        tooltipFormat: "yyyy-MM-dd",
                    },
                    title: {
                        display: true,
                        text: "Date (Quarterly)",
                    },
                },
                y: {
                    title: {
                        display: true,
                        text: "Value",
                    },
                },
            },
        };
        // Initialize and render the chart
        const chartHolder = document.getElementById("myChart").getContext("2d");
        return new Chart(chartHolder, {
            type: "line",
            data,
            options,
        });
    } catch (error) {
        console.error("Error creating chart:", error);
        // Display a user-friendly message if something goes wrong
        document.body.innerHTML =
            "<p>Failed to load chart data. Please try again later.</p>";
    }
}

loadHoldingsTrandsChart();
