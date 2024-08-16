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

export { removeDuplicatesByDate, aggregateData };
