import requests
from bs4 import BeautifulSoup
import csv

URL = "https://www.moneycontrol.com/stocks/marketstats/nse-mostactive-stocks/nifty-50-9/"
OUTPUT_FILE = "./post2.csv"


def get_perct():
    try:
        response = requests.get(URL)
        response.raise_for_status()  # Check for request errors
        html = response.text

        soup = BeautifulSoup(html, "html.parser")
        stock_percents = []

        for element in soup.select("tr td:nth-child(5)"):
            stock_perct = element.get_text()
            stock_percents.append(stock_perct)

        return stock_percents
    except requests.exceptions.RequestException as e:
        print(f"Error occurred while hitting URL: {e}")
        return []


if __name__ == "__main__":
    stock_percents = get_perct()
    if stock_percents:
        with open(OUTPUT_FILE, mode="w", newline="") as file:
            writer = csv.writer(file)
            for stock_perct in stock_percents:
                writer.writerow([stock_perct])
