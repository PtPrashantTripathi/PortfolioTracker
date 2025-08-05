// eslint.config.ts
import prettier from "eslint-config-prettier";
import unusedImports from "eslint-plugin-unused-imports";
import simpleImportSort from "eslint-plugin-simple-import-sort";
import globals from "globals";
import reactHooks from "eslint-plugin-react-hooks";
import reactRefresh from "eslint-plugin-react-refresh";
import js from "@eslint/js";
import ts from "typescript-eslint";
import { globalIgnores } from "eslint/config";

export default ts.config([
    globalIgnores(["dist", "node_modules"]),
    {
        files: ["**/*.ts", "**/*.tsx"],
        extends: [
            prettier,
            js.configs.recommended,
            ts.configs.recommended,
            reactHooks.configs["recommended-latest"],
            reactRefresh.configs.vite,
        ],
        languageOptions: {
            ecmaVersion: "latest",
            sourceType: "module",

            globals: {
                ...globals.browser,
                React: "readonly",
                JSX: "readonly",
            },
        },
        plugins: {
            "unused-imports": unusedImports,
            "simple-import-sort": simpleImportSort,
        },

        rules: {
            // "no-console": "warn",
            "no-unused-vars": "off",
            "@typescript-eslint/no-unused-vars": "off",
            "unused-imports/no-unused-imports": "warn",
            "unused-imports/no-unused-vars": [
                "warn",
                {
                    vars: "all",
                    varsIgnorePattern: "^_",
                    args: "after-used",
                    argsIgnorePattern: "^_",
                },
            ],
            "simple-import-sort/exports": "warn",
            "simple-import-sort/imports": "warn",
        },
    },
]);
