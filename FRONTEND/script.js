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

// Function to get data based on file type (CSV or JSON)
async function getData(filePath) {
  try {
    const response = await fetch(filePath);

    // Check if the file exists and was fetched successfully
    if (!response.ok) {
      throw new Error(`Failed to fetch the file: ${response.statusText}`);
    }

    // Handle CSV file
    if (filePath.endsWith(".csv")) {
      const csvData = await response.text();
      const parsedData = parseCSV(csvData); // Parse the CSV data
      return parsedData;

      // Handle JSON file
    } else if (filePath.endsWith(".json")) {
      const jsonData = await response.json();
      return jsonData;

      // Handle unknown file type
    } else {
      throw new Error(
        "Unknown data type. Please provide a valid CSV or JSON file."
      );
    }
  } catch (error) {
    console.error("Error fetching or processing the data:", error);
    throw error;
  }
}

async function loadHoldingsTrandsChart(data) {
  const options = {
    series: [
      {
        name: "Investment",
        data: data.map((d) => ({
          x: new Date(d.date),
          y: parseFloat(d.holding),
        })),
      },
      {
        name: "Market Value",
        data: data.map((d) => ({
          x: new Date(d.date),
          y: parseFloat(d.close),
        })),
      },
    ],
    // responsive: true,
    chart: {
      type: "area",
      height: "100%", // allows flexibility for responsive designs
      width: "100%",

      toolbar: {
        show: false,
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
      // tickAmount: 4,
      //   floating: false,
      labels: {
        formatter: (a) => priceConvert(a, true),
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
        formatter: (a) => priceConvert(a),
      },
    },
    legend: {
      show: false, // Hide legend
    },
    grid: {
      show: false, // Hides both x and y axis grid lines
    },
  };

  const chart = new ApexCharts(
    document.getElementById("holdingsTrandsChart"),
    options
  );

  chart.render();

  return chart;
}

// Function to find records with the maximum date
function findMaxDateRecords(data) {
  const maxDate = data.reduce((max, item) => {
    const currentDate = new Date(item.date);
    return currentDate > max ? currentDate : max;
  }, new Date(data[0].date));

  return data.filter(
    (item) => new Date(item.date).getTime() === maxDate.getTime()
  );
}

function priceConvert(value, short = false) {
  const amount = parseFloat(value);

  if (short) {
    if (amount >= 1e7) return `₹${(amount / 1e7).toFixed(2)}Cr`;
    if (amount >= 1e5) return `₹${(amount / 1e5).toFixed(2)}L`;
    if (amount >= 1e3) return `₹${(amount / 1e3).toFixed(2)}K`;
  }

  return amount.toLocaleString("en-IN", {
    maximumFractionDigits: 2,
    style: "currency",
    currency: "INR",
  });
}

function parseNum(value) {
  return parseFloat(value) === parseInt(value)
    ? parseInt(value)
    : parseFloat(parseFloat(value).toFixed(2));
}

async function loadProfitLossDataTable(data) {
  const tableBody = document.getElementById("ProfitLossTable");
  tableBody.innerHTML = ""; // Clear existing table rows
  data.forEach((record) => {
    const row = document.createElement("tr");
    const pnl_flag = parseFloat(record.pnl) < 0;
    // Create and append cells
    row.appendChild(createCell(`${record.scrip_name} (${record.segment})`));
    row.appendChild(createCell(parseNum(record.quantity)));
    row.appendChild(createCell(priceConvert(record.avg_price)));
    row.appendChild(createCell(priceConvert(record.sell_price)));
    row.appendChild(
      createCell(
        `${priceConvert(record.pnl)} (${pnl_flag ? "" : "+"}${parseNum(
          (parseFloat(record.pnl) * 100) /
            (parseNum(record.avg_price) * parseNum(record.quantity))
        )}%)`,
        pnl_flag ? ["text-danger"] : ["text-success"]
      )
    );
    row.appendChild(createCell(record.days));
    // Append the row to the table body
    tableBody.appendChild(row);
  });
}

async function loadHoldingsDataTable(data) {
  const table = document.getElementById("CurrentHoldingsTable"); // <table class="table m-0" id="CurrentHoldingsTable"></table>
  table.innerHTML = ""; // Clear existing table rows

  // Create and append the table header
  table.innerHTML = `
    <thead>
      <tr>
        <th>Stock Name</th>
        <th>Qty.</th>
        <th>Avg Price</th>
        <th>Invested</th>
        <th>CMP Price</th>
        <th>Current</th>
        <th>Unrealized PNL</th>
        <th>Days</th>
      </tr>
    </thead>
  `;

  const tableBody = document.createElement("tbody");

  data.forEach((record) => {
    const row = document.createElement("tr");
    const pnl =
      parseFloat(record.close_amount) - parseFloat(record.holding_amount);
    const pnlClass = pnl < 0 ? "text-danger" : "text-success";
    const pnlPercentage = `${priceConvert(pnl)} (${
      pnl >= 0 ? "+" : ""
    }${parseNum((pnl * 100) / record.holding_amount)}%)`;

    const cells = [
      createCell(`${record.scrip_name} (${record.segment})`),
      createCell(parseNum(record.holding_quantity)),
      createCell(priceConvert(record.avg_price)),
      createCell(priceConvert(record.holding_amount)),
      createCell(priceConvert(record.close_price), [pnlClass]),
      createCell(priceConvert(record.close_amount), [pnlClass]),
      createCell(pnlPercentage, [pnlClass]),
      createCell("TBD"),
    ];

    // Append all cells to the row
    cells.forEach((cell) => row.appendChild(cell));

    // Append the row to the table body
    tableBody.appendChild(row);
  });

  // Append the table body to the table
  table.appendChild(tableBody);
}

// Helper function to create a cell with text content and optional classes
function createCell(text, classes = []) {
  const cell = document.createElement("td");
  cell.innerHTML = text;
  classes.forEach((cls) => cell.classList.add(cls));
  return cell;
}

function updateFinancialSummary(data) {
  const { close, holding } = data;
  const pnl = close - holding;
  const pnlClass = pnl > 0 ? "bg-success" : "bg-danger";
  const pnlIcon = pnl > 0 ? "▲" : "▼";
  const pnlPercentage = parseNum((pnl * 100) / holding);

  const summaryItems = [
    {
      value: priceConvert(close),
      label: "Total Assets Worth",
      colorClass: "bg-info",
      iconClass: "fas fa-coins",
      link: "#",
    },
    {
      value: priceConvert(holding),
      label: "Total Investment",
      colorClass: "bg-warning",
      iconClass: "fas fa-cart-shopping",
      link: "#",
    },
    {
      value: priceConvert(pnl),
      label: "Total P&L",
      colorClass: pnlClass,
      iconClass: "fas fa-chart-pie",
      link: "#",
    },
    {
      value: `${pnlIcon}${pnlPercentage}%`,
      label: "Overall Return",
      colorClass: pnlClass,
      iconClass: "fas fa-chart-line",
      link: "#",
    },
  ];

  const elem = document.getElementById("FinancialSummary");
  elem.innerHTML = summaryItems
    .map(
      (item) => `
<div class="col-lg-3 col-6">
  <div class="small-box ${item.colorClass}">
    <div class="inner">
      <h4>${item.value}</h4>
      <h6>${item.label}</h6>
    </div>
    <div class="icon">
      <i class="${item.iconClass}"></i>
    </div>
    <a href="${item.link}" class="small-box-footer">
      More info <i class="fas fa-arrow-circle-right"></i>
    </a>
  </div>
</div>
`
    )
    .join("");
}

function updateProfitLossDataSummary(data) {
  const { sold, invested, pnl } = data;

  const elem = document.getElementById("PNLSummary");
  elem.innerHTML = `
        <!-- All Your Assets Worth -->
        <div class="col-sm-12 col-md-4 mb-3 mb-md-0">
            <div class="h4 ${
              pnl > 0 ? "text-success" : "text-danger"
            }">${priceConvert(sold, true)}</div>
            <small class="text-secondary">All your assets turnover</small>
        </div>
        <!-- Invested Amount -->
        <div class="col-sm-12 col-md-4 mb-3 mb-md-0">
            <div class="h4 text-primary">${priceConvert(invested, true)}</div>
            <small class="text-secondary">Invested amount</small>
        </div>
        <!-- Overall Returns -->
        <div class="col-sm-12 col-md-4">
            <div class="h4 ${
              pnl > 0 ? "text-success" : "text-danger"
            }">${priceConvert(pnl, true)} (${pnl > 0 ? "+" : ""}${parseNum(
    (pnl * 100) / invested
  )}%)</div>
            <small class="text-secondary">Overall Realized PNL</small>
        </div>
    `;
}

function find_base_path() {
  if (window.location.hostname === "ptprashanttripathi.github.io") {
    return "https://raw.githubusercontent.com/PtPrashantTripathi/PortfolioTracker/main/";
  } else {
    return "/";
  }
}

async function main() {
  const base_path = find_base_path();

  // Fetch the data using getData
  const holdingsTrandsFilePath = `${base_path}DATA/GOLD/Holdings/HoldingsTrands_data.csv`;
  const holdingsTrands_data = await getData(holdingsTrandsFilePath);
  loadHoldingsTrandsChart(holdingsTrands_data);

  const financialSummaryData = findMaxDateRecords(holdingsTrands_data)[0];
  updateFinancialSummary(financialSummaryData);

  // Fetch the data using getData
  const holdingsDataFilePath = `${base_path}DATA/GOLD/Holdings/HoldingsHistory_data.csv`;
  const holdingsData = await getData(holdingsDataFilePath);
  const filterdData = findMaxDateRecords(holdingsData);
  console.log(filterdData);
  loadHoldingsDataTable(filterdData);

  // Fetch the data using getData
  const profitLossDataFilePath = `${base_path}DATA/PRESENTATION/API/ProfitLoss_data.json`;
  const profitLossData = await getData(profitLossDataFilePath);
  loadProfitLossDataTable(profitLossData.data);
  const profitLossSummaryData = profitLossData;
  updateProfitLossDataSummary(profitLossSummaryData);
}

main();
