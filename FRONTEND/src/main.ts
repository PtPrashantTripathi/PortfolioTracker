// src/main.ts

// A sample function to export
export function greet(name: string): string {
    return `Hello, ${name}!`;
}

// Main logic (using the greet function)
const messageElement = document.getElementById("message");
if (messageElement) {
    messageElement.textContent = greet("Prashant");
}
