#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 17 22:54:10 2018

@author: darkrider
"""

import os, time, datetime, re, copy
import pandas as pd
import numpy as np
from selenium import webdriver
from itertools import groupby


# directory = "/Users/darkrider/Desktop/zerodha_tradepnl_company_action_adjusted/"
# os.chdir(directory)


# path_to_chromedriver = directory + "chromedriver"
path_to_chromedriver = "./chromedriver"


options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")
prefs = {
    "profile.default_content_settings.popups": 0,
    "download.default_directory": "./",  # IMPORTANT - ENDING SLASH V IMPORTANT
    "directory_upgrade": True,
}
options.add_experimental_option("prefs", prefs)
browser = webdriver.Chrome(executable_path=path_to_chromedriver, chrome_options=options)
split_url = "http://www.moneycontrol.com/stocks/marketinfo/splits/index.php?sel_year="
bonus_url = "http://www.moneycontrol.com/stocks/marketinfo/bonus/index.php?sel_year="
url = [split_url, bonus_url]


def load_data(fileName):
    excel = pd.ExcelFile(fileName).parse("TRADEBOOK")
    excel = excel.iloc[:, 1:].dropna(axis=0)
    excel.reset_index(inplace=True, drop=True)
    excel.columns = excel.iloc[0]
    excel = excel.reindex(excel.index.drop(0))
    if "ALL-EQ" in fileName:
        excel = excel.sort_values(["Symbol"])
    excel.reset_index(drop=True, inplace=True)
    excel["Trade date"] = pd.to_datetime(excel["Trade date"], format="%d-%m-%Y").dt.date
    excel = excel[["Symbol", "Trade date", "Type", "Qty", "Rate"]]
    excel.columns = ["symbol", "date", "type", "qty", "rate"]
    return excel


def standardize_dates(tradeDF):
    yearArray = None
    try:
        tradeDF["date"] = pd.to_datetime(tradeDF["date"]).dt.date
        yearArray = np.unique(pd.to_datetime(tradeDF["date"]).dt.year)
    except ValueError:
        pass
    return (tradeDF, yearArray)


fileName = [x for x in os.listdir() if ".xlsx" in x][0]  #'TRADEBOOK ALL-EQ 01_04_2017 TO 17_05_2018.xlsx'
tradeDF = load_data(fileName)
tradeDF, years = standardize_dates(tradeDF)


# current year should be added to account for Corp Actions being taken currently
(yearT, monthT, dayT) = tuple(datetime.datetime.today().strftime("%Y-%m-%d").split("-"))
if int(yearT) not in years:
    years = np.append(years, 2018)


def getBseCorpAct(browser):
    browser.get("https://www.bseindia.com/corporates/corporate_act.aspx")

    browser.find_element_by_name("ctl00$ContentPlaceHolder1$txtDate").click()
    browser.execute_script("javascript:setCalendarControlDate(" + str(years[0]) + ",1,1)")

    browser.find_element_by_name("ctl00$ContentPlaceHolder1$txtTodate").click()
    browser.execute_script("javascript:setCalendarControlDate(" + str(yearT) + "," + str(monthT).lstrip("0") + "," + str(dayT).lstrip("0") + ")")

    browser.find_element_by_name("ctl00$ContentPlaceHolder1$btnSubmit").click()
    # directly downloads to preferref location set in driver preferences
    browser.execute_script("javascript:__doPostBack('ctl00$ContentPlaceHolder1$lnkDownload','')")
    while "Corporate_Actions.csv" not in os.listdir():
        time.sleep(1)
    # data = pd.read_csv(directory + "Corporate_Actions.csv", index_col = None)
    data = pd.read_csv("./Corporate_Actions.csv", index_col=None)
    data.columns = [x.lower().replace(" ", "_").replace("\t", "") for x in data.columns]
    data["purpose"] = data.purpose.apply(lambda x: x.lower())
    data = data[[True if "split" in x or "bonus" in x else False for x in data["purpose"]]]
    data.reset_index(drop=True, inplace=True)
    # data.to_csv(directory + "Corporate_Actions.csv", index = None)
    data.to_csv("./Corporate_Actions.csv", index=None)


def getIDentifiers(link, browser):
    print(len([x for x in link.split("/") if x is not ""]))
    if len([x for x in link.split("/") if x is not ""]) != 7:
        return pd.DataFrame([[np.nan] * 4], columns=["bse", "nse", "isin", "sector"])
    browser.get(link)
    print(link)
    time.sleep(2)
    idList = browser.find_element_by_xpath('//div[@class = "FL gry10"]').text.split("|")[:-1]
    idList = [x.strip().split(":") for x in idList]
    return pd.DataFrame.from_dict({val[0].strip().lower(): [val[1].strip()] for val in idList})


def getCorpActData(url, years, act):
    i = 0
    mssg = "Getting Data for Splits"
    if act == "Bonus":
        i = 1
        mssg = "Getting Data for Bonus"
    final_df = None
    for year in years:
        print(mssg)
        link = url[i] + str(year)
        browser.get(link)
        time.sleep(3)
        content = browser.find_element_by_xpath('//div[@class="MT15"]/table[@class="b_12 dvdtbl tbldata14"]').get_attribute("outerHTML")
        links = browser.find_elements_by_xpath('//div[@class="MT15"]/table[@class="b_12 dvdtbl tbldata14"]//td[@class="dvd_brdb"]/a')
        links = [x.get_attribute("href") for x in links]
        links = links
        df = pd.read_html(content)[0]
        if i == 0:
            df.drop([0], inplace=True)
            df.reset_index(drop=True, inplace=True)
            df.columns = ["company", "old", "new", "split_date"]
            idListDf = [getIDentifiers(x, browser) for x in links]
            idDf = pd.concat(idListDf, axis=0)
            idDf.reset_index(drop=True, inplace=True)
            df = pd.concat([df, idDf], axis=1)
        else:
            df.drop([0, 1], inplace=True)
            df.reset_index(drop=True, inplace=True)
            df.columns = ["company", "bonus_ratio", "ex_bonus_date", "announcement_date", "record_date"]
            bonusDf = pd.DataFrame(df["bonus_ratio"].apply(lambda x: x.split(":")).tolist(), columns=["old", "new"])
            df = pd.concat([df, bonusDf], axis=1)
            idListDf = [getIDentifiers(x, browser) for x in links]
            idDf = pd.concat(idListDf, axis=0)
            idDf.reset_index(drop=True, inplace=True)
            df = pd.concat([df, idDf], axis=1)
        df["company"] = df["company"].apply(lambda x: str(x).replace("  Add to Watchlist  Add to Portfolio", ""))
        final_df = pd.concat([final_df, df], axis=0)
    return final_df


def adjustmentCalculator(data, typeOf, old, new):
    if typeOf == "split":
        data["rate"] = data["rate"] * (new / old)
        data["qty"] = data["qty"] * (old / new)
    elif typeOf == "bonus":  # will include other as well soon like reverse split
        cols = list(data.columns)
        data["remainder"] = [x % new for x in data["qty"]]
        data["quotient"] = [int(x / new) for x in data["qty"]]
        data["rate"] = data["qty"] * data["rate"] / (data["qty"] + data["quotient"])
        data["qty"] = data["qty"] + data["quotient"]
        data = data[cols]
    return data


def pnlCalculator(data):
    data.reset_index(drop=True, inplace=True)
    # Declaring variables to calc pnl
    buy_avg = 0
    buy_pos = 0
    sell_avg = 0
    sell_pos = 0
    realized_profit = []

    # out of tuple of index and row, choose row and select a column
    # print(np.unique(data['type']))
    # print(len(list(groupby(data.iterrows(), key=lambda row: row[1]['type']))))
    for i, (k, l) in enumerate(groupby(data.iterrows(), key=lambda row: row[1]["type"])):
        sub_df = data.iloc[[t[0] for t in l], :]
        # print(i,k,sub_df)
        sub_df.reset_index(inplace=True, drop=True)
        # print(i,k)
        if i == 0:
            if k == "B":
                buy_pos = sum(sub_df["qty"])
                buy_avg = sum(sub_df["qty"] * sub_df["rate"]) / buy_pos
            else:
                sell_pos = sum(sub_df["qty"])
                sell_avg = sum(sub_df["qty"] * sub_df["rate"]) / sell_pos
        elif k == "B":
            if sell_pos > 0:  # intraday trades
                assert buy_pos == 0
                # print("Was here Buy")
                if (sell_pos - sum(sub_df["qty"])) >= 0:
                    sell_pos -= sum(sub_df["qty"])
                    buy_avg = sum(sub_df["qty"] * sub_df["rate"]) / sum(sub_df["qty"])
                    # realized_profit
                    try:
                        realized_profit.append((sum(sub_df["qty"])) * (sell_avg - buy_avg))
                    except Exception as e:
                        pass
                else:
                    sub_df["cum_qty"] = sell_pos - sub_df["qty"].cumsum()
                    minIndex = np.min(np.where(sub_df["cum_qty"] <= 0))

                    df1 = sub_df[: minIndex + 1].reset_index(drop=True)
                    df1.iloc[-1, df1.columns.get_loc("qty")] = int(df1.tail(1)["qty"] + df1.tail(1)["cum_qty"])
                    buy_avg = sum(df1["qty"] * df1["rate"]) / sum(df1["qty"])

                    # realized_profit
                    try:
                        realized_profit.append((sum(df1["qty"])) * (sell_avg - buy_avg))
                    except Exception as e:
                        pass

                    df2 = sub_df[minIndex:].reset_index(drop=True)
                    df2.iloc[0, df2.columns.get_loc("qty")] = int(-1 * df2.head(1)["cum_qty"])
                    buy_avg = sum(df2["qty"] * df2["rate"]) / sum(df2["qty"])

                    buy_pos = sum(sub_df["qty"]) - sell_pos
                    sell_pos = 0

            else:
                buy_avg = (sum(sub_df["qty"] * sub_df["rate"]) + buy_avg * buy_pos) / (buy_pos + sum(sub_df["qty"]))
                buy_pos = buy_pos + sum(sub_df["qty"])

        elif k == "S":
            if buy_pos > 0:
                assert sell_pos == 0
                # print("Was here Sell")
                if (buy_pos - sum(sub_df["qty"])) >= 0:
                    buy_pos -= sum(sub_df["qty"])
                    sell_avg = sum(sub_df["qty"] * sub_df["rate"]) / sum(sub_df["qty"])
                    # realized_profit
                    try:
                        realized_profit.append((sum(sub_df["qty"])) * (sell_avg - buy_avg))
                    except Exception as e:
                        pass
                else:
                    sub_df["cum_qty"] = buy_pos - sub_df["qty"].cumsum()
                    minIndex = np.min(np.where(sub_df["cum_qty"] <= 0))

                    df1 = sub_df[: minIndex + 1].reset_index(drop=True)
                    df1.iloc[-1, df1.columns.get_loc("qty")] = int(df1.tail(1)["qty"] + df1.tail(1)["cum_qty"])
                    sell_avg = sum(df1["qty"] * df1["rate"]) / sum(df1["qty"])

                    # realized_profit
                    try:
                        realized_profit.append((sum(df1["qty"])) * (sell_avg - buy_avg))
                    except Exception as e:
                        pass

                    df2 = sub_df[minIndex:].reset_index(drop=True)
                    df2.iloc[0, df2.columns.get_loc("qty")] = int(-1 * df2.head(1)["cum_qty"])
                    sell_avg = sum(df2["qty"] * df2["rate"]) / sum(df2["qty"])

                    sell_pos = sum(sub_df["qty"]) - buy_pos
                    buy_pos = 0
            else:
                sell_avg = (sum(sub_df["qty"] * sub_df["rate"]) + sell_avg * sell_pos) / (sell_pos + sum(sub_df["qty"]))
                sell_pos = sell_pos + sum(sub_df["qty"])

    return sum(realized_profit)


# splitDF = getCorpActData(url, years, "Split")
# splitDF.to_csv(directory + "split.csv", index = None)
# splitDF.to_csv("./split.csv", index = None)
# bonusDF = getCorpActData(url, years, "Bonus")
# bonusDF.to_csv("./bonus.csv", index = None)
getBseCorpAct(browser)

# Loading the datasets
# splitDF = pd.read_csv(directory + "split.csv", index_col = None)
splitDF = pd.read_csv("./split.csv", index_col=None)
splitDF["split_date"] = pd.to_datetime(splitDF["split_date"], format="%d-%m-%Y").dt.date
# Keep only records where we have nse symbol, getting bse symbol from bse website
splitDF.dropna(axis=0, subset=["nse"], inplace=True)
splitDF.reset_index(drop=True, inplace=True)
splitDF = splitDF[["nse", "split_date", "old", "new"]]
splitDF.columns = ["symbol", "date", "old", "new"]
splitDF["type"] = "split"


# bonusDF = pd.read_csv(directory + "bonus.csv", index_col = None)
bonusDF = pd.read_csv("./bonus.csv", index_col=None)
bonusDF["split_date"] = pd.to_datetime(bonusDF["record_date"], format="%d-%m-%Y").dt.date
# Keep only records where we have nse symbol, getting bse symbol from bse website
bonusDF.dropna(axis=0, subset=["nse"], inplace=True)
bonusDF.reset_index(drop=True, inplace=True)
bonusDF = bonusDF[["nse", "split_date", "old", "new"]]
bonusDF.columns = ["symbol", "date", "old", "new"]
bonusDF["type"] = "bonus"


# bseCorpDF = pd.read_csv(directory + "Corporate_Actions.csv", index_col = None)
bseCorpDF = pd.read_csv("./Corporate_Actions.csv", index_col=None)
bseCorpDF["ex_date_form"] = pd.to_datetime(bseCorpDF["ex_date"], format="%d %b %Y").dt.date

# Creating split and bonuses
bseCorpSplitDf = bseCorpDF[["split" in x for x in bseCorpDF.purpose]]
bseCorpSplitDf.reset_index(drop=True, inplace=True)
bseCorpSplitDf = bseCorpSplitDf[["security_name", "ex_date_form", "purpose"]]
old_new = pd.DataFrame(bseCorpSplitDf.purpose.apply(lambda x: re.findall(r"\d+", x)[-2:]).tolist(), columns=["old", "new"])
bseCorpSplitDf = pd.concat([bseCorpSplitDf, old_new], axis=1)
bseCorpSplitDf = bseCorpSplitDf[["security_name", "ex_date_form", "old", "new"]]
bseCorpSplitDf.columns = ["symbol", "date", "old", "new"]
bseCorpSplitDf["type"] = "split"

bseCorpBonusDf = bseCorpDF[["bonus" in x for x in bseCorpDF.purpose]]
bseCorpBonusDf.reset_index(drop=True, inplace=True)
bseCorpBonusDf = bseCorpBonusDf[["security_name", "ex_date_form", "purpose"]]
old_new = pd.DataFrame(bseCorpBonusDf.purpose.apply(lambda x: re.findall(r"\d+", x)[:2]).tolist(), columns=["old", "new"])
bseCorpBonusDf = pd.concat([bseCorpBonusDf, old_new], axis=1)
bseCorpBonusDf = bseCorpBonusDf[["security_name", "ex_date_form", "old", "new"]]
bseCorpBonusDf.columns = ["symbol", "date", "old", "new"]
bseCorpBonusDf["type"] = "bonus"


corpAcDF = pd.concat([splitDF, bseCorpSplitDf, bonusDF, bseCorpBonusDf], axis=0, ignore_index=True)
corpAcDF["symbol"] = [x.strip() for x in corpAcDF["symbol"]]
corpAcDF["old"] = [int(x) for x in corpAcDF["old"]]
corpAcDF["new"] = [int(x) for x in corpAcDF["new"]]
corpAcDF.drop_duplicates(inplace=True)
corpAcDF.reset_index(inplace=True, drop=True)

# Getting unique symbols in my list and adjusting them for corporate action
symbols = np.unique(tradeDF.symbol)
symbolGroup = tradeDF.groupby(["symbol"])
symbolsCorpAcGroup = corpAcDF.groupby(["symbol"])

symbolGroupDict = {}
for symbol in symbols:
    print(symbol)
    # Tradebook
    df = symbolGroup.get_group(symbol)
    df.sort_values("date", inplace=True)
    df.reset_index(inplace=True, drop=True)
    if len(np.unique(df["type"])) > 1:
        # print(symbol,"Length Greater")
        if any([symbol in x for x in corpAcDF["symbol"]]):
            try:
                dfCorp = symbolsCorpAcGroup.get_group(symbol)
            except KeyError as e:
                symbolGroupDict[symbol] = pnlCalculator(data=df)
                continue
            # corporate Action
            dfCorp.sort_values("date", inplace=True)
            dfCorp.reset_index(inplace=True, drop=True)
            for _, row in dfCorp.iterrows():
                try:
                    tillIndex = np.min(np.where(df.date == row["date"]))
                except ValueError as e:
                    continue
                data = df.iloc[:tillIndex].reset_index(drop=True)
                data = adjustmentCalculator(data, row["type"], row["old"], row["new"])
                df.iloc[:tillIndex] = data

        symbolGroupDict[symbol] = pnlCalculator(data=df)
    else:
        # print(symbol,"Length Lower")
        symbolGroupDict[symbol] = None

pnlData = pd.DataFrame.from_dict(symbolGroupDict, orient="index")
pnlData["symbol"] = pnlData.index
pnlData.columns = ["realized_profit", "symbol"]
pnlData.to_csv("./result_pnl_output.csv", index=None)
