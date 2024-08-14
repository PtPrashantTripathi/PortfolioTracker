// export default {
//   title: "Multiple Bounded Areas (Natural Curve)",
//   legend: {
//     enabled: false,
//   },
//   bounds: {
//     upperBoundMapsTo: "max",
//     lowerBoundMapsTo: "min",
//   },
//   axes: {
//     bottom: {
//       title: "2023 Annual Sales Figures",
//       mapsTo: "date",
//       scaleType: "time",
//     },
//     left: {
//       mapsTo: "value",
//       scaleType: "linear",
//     },
//   },
//   curve: "curveNatural",
//   height: "400px",
// };

export default {
  title: 'Line (time series) - Time interval monthly with French locale',
  axes: {
    left: {
      ticks: {
      },
      mapsTo: 'value'
    },
    bottom: {
      scaleType: 'time',
      ticks: {
      },
      mapsTo: 'date'
    }
  },
  tooltip: {
  },
  legend: {
    clickable: false
  },
  height: '400px',
  locale: {
    code: 'fr-FR'
  }
}
