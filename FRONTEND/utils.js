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
