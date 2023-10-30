import re

stock_names_dict = {
    500112: "SBIN",
    500209: "INFY",
    500400: "TATAPOWER",
    500547: "BPCL",
    500570: "TATAMOTORS",
    500575: "VOLTAS",
    500770: "TATACHEM",
    530803: "BHAGERIA",
    532461: "PNB",
    532648: "YESBANK",
    532822: "IDEA",
    543266: "HERANBA",
    543526: "LICI",
    590095: "GOLDBEES",
    590103: "NIFTYBEES",
}


# fun to Removing punctuations from the columns
def replace_punctuation_from_columns(columns):
    clean_columns = []
    for each in columns:
        # Remove '_' as it replace space 'Stock Name' => stock_name
        clean_column_name = (
            re.compile("[%s]" % re.escape(r"""!"#$%&'()*+,-./:;<=>?@[\]^`{|}~"""))
            .sub("", each.strip())
            .strip()
            .replace(" ", "_")
            .lower()
        )
        while "__" in clean_column_name:
            clean_column_name = clean_column_name.replace("__", "_")
        clean_columns.append(clean_column_name)
    return clean_columns


class TradeHistory:
    def __init__(self, stock_name):
        self.stock_name = stock_name
        self.trade_price = list()
        self.trade_quantity = list()

    def fifo_sell_calc(self, trade_quantity):
        old_self = self.trade_quantity.copy()
        for i in range(len(self.trade_price)):
            if i == 0:
                self.trade_quantity[i] -= trade_quantity
            else:
                if self.trade_quantity[i - 1] < 0:
                    self.trade_quantity[i] += self.trade_quantity[i - 1]
                    self.trade_quantity[i - 1] = 0
                else:
                    break
        buy_price = 0
        for i in range(len(self.trade_quantity)):
            buy_price += (old_self[i] - self.trade_quantity[i]) * self.trade_price[i]
        return buy_price / trade_quantity

    def holding_quantity(self):
        return sum(self.trade_quantity)

    def calc_avg_price(self):
        invested = 0
        for i in range(len(self.trade_quantity)):
            invested += self.trade_quantity[i] * self.trade_price[i]
        if self.holding_quantity() != 0:
            return invested / self.holding_quantity()
        return 0
