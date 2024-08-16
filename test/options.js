export default {
  title: "Investment vs Market Value",
  axes: {
    left: {
      mapsTo: "value",
      stacked: true,
    },
    bottom: {
      mapsTo: "date",
      scaleType: "time",
    },
  },
  // curve: 'curveMonotoneX',
  height: "400px",
};
