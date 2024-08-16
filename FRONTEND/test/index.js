// Load Carbon Charts as Charts (UMD)
import "https://unpkg.com/@carbon/charts@latest/dist/umd/bundle.umd.js";
import options from "./options.js";
import getData from "./get_data.js";

// Get reference to chart holder DOM element
const chartHolder = document.getElementById("app");

function removeDuplicatesByDate(data) {
  // Sort the array by date
  data.sort((a, b) => new Date(a.date) - new Date(b.date));

  const result = [];
  let currentValue = null;
  let startIndex = null;

  for (let i = 0; i < data.length; i++) {
    const item = data[i];

    // If the value changes, process the previous sequence
    if (Math.floor(item.value / 10) !== Math.floor(currentValue / 10)) {
      if (startIndex !== null) {
        // Push the first and last item of the previous sequence
        result.push(data[startIndex]);
        if (startIndex !== i - 1) {
          result.push(data[i - 1]);
        }
      }
      // Reset for the new sequence
      currentValue = item.value;
      startIndex = i;
    }
  }

  // Push the last sequence if it exists
  if (startIndex !== null) {
    result.push(data[startIndex]);
    if (startIndex !== data.length - 1) {
      result.push(data[data.length - 1]);
    }
  }

  return result;
}

// Asynchronous function to fetch and render the chart
async function main() {
  // Fetch the data using getData
  const filePath = "../DATA/GOLD/HoldingsTrands/HoldingsTrands_data.csv";
  const raw_data = await getData(filePath);

  // map data
  const holdingData = removeDuplicatesByDate(
    raw_data.map((item) => ({
      group: "Investment",
      date: new Date(item.date),
      value: parseFloat(item.holding),
    }))
  );
  const closeData = removeDuplicatesByDate(
    raw_data.map((item) => ({
      group: "Market Value",
      date: new Date(item.date),
      value: parseFloat(item.close),
    }))
  );

  const mappedData = holdingData.concat(closeData);

  // Create a new AreaChart with the fetched data and provided options
  new Charts.LineChart(chartHolder, {
    data: mappedData,
    options: options,
  });
}

// Call the function to render the chart
main();
