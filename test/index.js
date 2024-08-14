// Load Carbon Charts as Charts (UMD)
import "https://unpkg.com/@carbon/charts@latest/dist/umd/bundle.umd.js";
import options from "./options.js";
import getData from "./get_data.js";

// Get reference to chart holder DOM element
const chartHolder = document.getElementById("app");

// Asynchronous function to fetch and render the chart
async function main() {
  // Fetch the data using getData
  const filePath = "../DATA/GOLD/HoldingsTrands/HoldingsTrands_data.csv";
  const raw_data = await getData(filePath);

  // map data
  const mappedData = raw_data.map((item) => ({
    group: "data",
    date: item.date,
    value: parseFloat(item.close), // item.holding
  }));


  // Create a new AreaChart with the fetched data and provided options
  new Charts.AreaChart(chartHolder, {
    data: mappedData,
    options: options,
  });
}

// Call the function to render the chart
main();
