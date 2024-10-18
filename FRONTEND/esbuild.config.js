// esbuild.config.js

require("esbuild")
    .build({
        entryPoints: ["src/main.ts"],
        outfile: "dist/main.js",
        bundle: true, // Bundle dependencies into one file (optional for larger projects)
        minify: false, // Minify the output (optional)
        sourcemap: true, // Helpful for debugging
        target: "es6", // Target modern JavaScript (ES6+)
        platform: "browser", // Target browser environment
        // watch: process.argv.includes('--watch'),  // Enable watch mode if --watch flag is used
    })
    .catch(() => process.exit(1));
