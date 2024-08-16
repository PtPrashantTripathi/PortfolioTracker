export default {
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
};
