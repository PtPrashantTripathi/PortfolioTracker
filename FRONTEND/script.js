function smartConvert(value) {
    // Try to convert to float
    const floatValue = parseFloat(value);
    if (!isNaN(floatValue) && !Number.isInteger(floatValue)) {
        return floatValue;
    }

    // Try to convert to integer
    const intValue = parseInt(value);
    if (!isNaN(intValue)) {
        return intValue;
    }

    // If all else fails, return as string
    return String(value);
}

// Function to parse CSV data
function parseCSV(csv) {
    const lines = csv.trim().split("\n");
    const headers = lines[0].split(",").map((header) => header.trim()); // Trim all headers
    const result = [];

    for (let i = 1; i < lines.length; i++) {
        const obj = {};
        const currentLine = lines[i].split(",");

        headers.forEach((header, index) => {
            obj[header] = currentLine[index].trim();
        });

        result.push(obj);
    }

    return result;
}

// Function to get data
async function getData(filePath) {
    const response = await fetch(filePath);
    const csvData = await response.text(); // Await the fetched CSV data
    const parsedData = parseCSV(csvData); // Parse the CSV data
    return parsedData;
}

async function loadHoldingsTrandsChart(chartHolder) {
    try {
        // Fetch the data using getData
        const filePath = "../DATA/GOLD/Holdings/HoldingsTrands_data.csv";
        const data = await getData(filePath);

        // Initialize and render the chart
        return new Chart(chartHolder, {
            type: "line",
            data: {
                labels: data.map((d) => new Date(d.date)),
                datasets: [
                    {
                        label: "Investment",
                        data: data.map((d) => parseFloat(d.holding)),
                        borderColor: "rgba(58,45,127)",
                        backgroundColor: "rgba(58,45,127)",
                        borderWidth: 2,
                        fill: false,
                        tension: 1,
                    },
                    {
                        label: "Market Value",
                        data: data.map((d) => parseFloat(d.close)),
                        borderColor: "rgb(0,208,156)",
                        backgroundColor: "rgb(0,208,156)",
                        borderWidth: 2,
                        fill: {
                            target: "origin",
                            above: "rgb(0,208,156,0.5)",
                        },
                        tension: 1,
                    },
                ],
            },
            options: {
                elements: {
                    point: {
                        pointStyle: true,
                        pointRadius: 0,
                        pointHoverRadius: 5,
                    },
                },
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
                            text: "Price (INR)",
                        },
                        beginAtZero: false,
                    },
                },
            },
        });
    } catch (error) {
        console.error("Error creating chart:", error);
        // Display a user-friendly message if something goes wrong
        document.body.innerHTML =
            "<p>Failed to load chart data. Please try again later.</p>";
    }
}
