<p align="center">
  <a href="https://github.com/portfoliotracker/portfoliotracker"><img src="https://portfoliotracker.io/icons/icon-512x512.png" alt="PortfolioTracker is a free & open-source HTML dashboard & admin template based on Bootstrap 5" width="96"></a>
</p>

<h3 align="center">PortfolioTracker</h3>

<p align="center">
  PortfolioTracker is a free & open-source HTML dashboard & admin template based on Bootstrap 5.
</p>

<p align="center">
  <a href="https://www.npmjs.com/package/@portfoliotracker/core" rel="nofollow"><img src="https://img.shields.io/npm/v/@portfoliotracker/core" alt="NPM version"></a>
  <a href="https://github.com/portfoliotracker/portfoliotracker/blob/master/LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License"></a>
  <a href="https://github.com/portfoliotracker/portfoliotracker/issues?q=is%3Aopen+is%3Aissue"><img src="https://img.shields.io/github/issues/portfoliotracker/portfoliotracker.svg" alt="GitHub issues open"></a>
  <a href="https://github.com/portfoliotracker/portfoliotracker/issues?q=is%3Aissue+is%3Aclosed"><img src="https://img.shields.io/github/issues-closed-raw/portfoliotracker/portfoliotracker.svg" alt="GitHub issues closed"></a>
  <a href="https://demo.portfoliotracker.io/" rel="nofollow"><img src="https://img.shields.io/badge/demo-online-green.svg" alt="Live Demo"></a>
  <a href="https://portfoliotracker.io/#download"><img src="https://img.shields.io/static/v1?label=download&message=ZIP&color=green" alt="Download"></a>
  <br>
<a href="https://github.com/portfoliotracker/portfoliotracker"><img alt="GitHub stars" src="https://img.shields.io/github/stars/portfoliotracker/portfoliotracker?style=social"></a>
</p>

## Preview
A professional Admin & Dashboard template based on Bootstrap 5 that comes with hundreds of UI components, forms, tables, charts, pages and icons. PortfolioTracker **does not require jQuery** and neither does one of the 3rd party libraries PortfolioTracker is using. [Open demo](https://demo.portfoliotracker.io/).

<a href="https://demo.portfoliotracker.io" target="_blank"><img src="https://assets.portfoliotracker.io/banners/github-1280%C3%97640px.png?2" alt="PortfolioTracker Demo"></a>

## Features

* **Customizable:** You don't need to be an expert to customize PortfolioTracker. Our code is very readable and well documented.
* **Fully Responsive:** With mobile, tablet & desktop support it doesn't matter what device you're using. PortfolioTracker is responsive in all browsers.
* **Cross-Browser:** Our themes are working perfectly with Chrome, Firefox, Safari, Opera, and Edge. We're working hard to support them.
* **Clean Code:** We strictly follow Bootstrap's guidelines to make your integration as easy as possible. All code is handwritten.
* **No jQuery:** PortfolioTracker, and all third-party libraries used in the admin template, do not require jQuery as a dependency.
* **Regular Updates:** From time to time you'll receive an update containing new components, improvements, and bugfixes.

## Quick start

### Download

* Clone this repo `git clone https://github.com/portfoliotracker/portfoliotracker.git`
* [Download from GitHub](https://github.com/portfoliotracker/portfoliotracker/archive/master.zip)
* [Download from Website](https://portfoliotracker.io/#download)

### Build tools

The theme includes a custom Webpack file, which can be used to quickly recompile and minify theme assets while developing or for deployment. You'll need to install Node.js before using Webpack.

Once Node.js is installed, run npm install to install the rest of PortfolioTracker's dependencies. All dependencies will be downloaded to the node_modules directory.

```sh
npm install
```

Now you're ready to modify the source files and generate new dist/ files. PortfolioTracker uses webpack-dev-server to automatically detect file changes and start a local webserver at http://localhost:8080.

```sh
npm start
```

Compile, optimize, minify and uglify all source files to dist/ folder:

```sh
npm run build
```

## CDN support

All files included in the `@portfoliotracker/core` npm package are available over a CDN.

**CSS:**

```html
<link rel="stylesheet" href="https://unpkg.com/@portfoliotracker/core@latest/dist/css/app.css">
```

**Javascript:**

```html
<script src="https://unpkg.com/@portfoliotracker/core@latest/dist/js/app.js"></script>
```

## File structure
The package contains the following directories and files:

```
portfoliotracker/
├── .babelrc
├── .eslintrc
├── .nvmrc
├── README.md
├── package.json
├── postcss.config.js
├── webpack.config.js
├── src/
│   ├── fonts/
│   ├── img/
│   ├── js/
│   │   ├── modules/
│   │   └── app.js
│   └── scss/
│       ├── 1-variables/
│       ├── 2-mixins/
│       ├── 3-components/
│       ├── 4-utilities/
│       ├── 5-vendor/
│       └── app.scss
└── dist/
    ├── css/
    │   └── app.css
    └── js/
        └── app.js
```

## Browser Support

| <img src="https://assets.portfoliotracker.io/browsers/edge.png" alt="Edge" width="24px" height="24px" /><br/>Edge | <img src="https://assets.portfoliotracker.io/browsers/firefox.png" alt="Firefox" width="24px" height="24px" /><br/>Firefox | <img src="https://assets.portfoliotracker.io/browsers/chrome.png" alt="Chrome" width="24px" height="24px" /><br/>Chrome | <img src="https://assets.portfoliotracker.io/browsers/safari.png" alt="Safari" width="24px" height="24px" /><br/>Safari | <img src="https://assets.portfoliotracker.io/browsers/safari-ios.png" alt="iOS Safari" width="24px" height="24px" /><br/>iOS Safari |
| --------- | --------- | --------- | --------- | --------- |
| last version| last 2 versions| last 2 versions| last 2 versions| last 2 versions

## Resources

* [Demo](https://demo.portfoliotracker.io/)
* [Website](https://portfoliotracker.io/)
* [Documentation](https://portfoliotracker.io/docs)
* [Support](https://portfoliotracker.io/support/)
* [Affiliate Program](https://portfoliotracker.io/affiliate-program)

## Bugs and feature requests

Found a bug or have a feature request? [Please open a new issue](https://github.com/portfoliotracker/portfoliotracker/issues/new).

## Upgrade to PRO
Get more power with PortfolioTracker PRO, a premium variant of PortfolioTracker, featuring hundreds of UI components, forms, tables, charts, pages, and icons.

| Free Version        | [PortfolioTracker PRO](https://portfoliotracker.io/pricing/) |
|---------------------|----------------------------------------------|
| 15 Demo Pages       | ✔ 45+ Demo Pages                             |
| 1 Plugin            | ✔ 10+ Plugins                                |
| 1 Color Scheme      | ✔ 3 Color Schemes                            |
|                     | ✔ All Bootstrap 5 Components                 |
|                     | ✔ Dark Mode 🌙                               |
|                     | ✔ Compact Sidebar                            |
|                     | ✔ Calendar                                   |
|                     | ✔ Advanced Forms                             |
|                     | ✔ Drag and Drop                              |
|                     | ✔ Toast Notifications                        |
|                     | ✔ WYSIWYG Editors                            |
|                     | ✔ Premium Support                            |
