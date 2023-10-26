class Runner:
    def __init__(self, data, start_money):
        self.data = data
        self.start_money = start_money
        self.avail_money = start_money
        self.current_stocks = 0
        self.profit_percent = None
        self.transactions = 0
        self.last_buy_time = 0
        self.total_time_invested = 0

    def buy_max(self, price, time):
        buying_amount = self.avail_money // price
        if buying_amount:
            self.transactions = self.transactions + 1
            self.last_buy_time = time
            self.current_stocks = self.current_stocks + buying_amount
            self.avail_money = self.avail_money - price * buying_amount

    def sell_max(self, price, time):
        if self.current_stocks:
            self.transactions = self.transactions + 1
            self.total_time_invested = (
                self.total_time_invested + time - self.last_buy_time
            )
            self.avail_money = self.avail_money + price * self.current_stocks
            self.current_stocks = 0

    def run_strat(
        self,
        buy_on_diff_percent_min,
        buy_on_diff_percent_max,
        sell_on_diff_percent_min,
        sell_on_diff_percent_max,
        agg,
        delay_milis=5000,
    ):
        item_list=[]
        set_buy = None
        set_sell = None
        transaction_time = None
        aggregate = agg
        for item in self.data:
            item_list.append(item)
            time = item[0]
            price = item[1]

            old_items_index=None
            for i in range(len(item_list)):
                if item_list[i][0] + aggregate < item[0]:
                    old_items_index = i
                else:
                    break
            if old_items_index is not None:
                if old_items_index+2 == len(item_list):
                    #Keeps at least 1 object
                    old_items_index = old_items_index - 1
                item_list=item_list[old_items_index+1:]
            previous_price = item_list[0][1]
            price_diff = ((price / previous_price) - 1) * 100
            if transaction_time is not None:
                if time > transaction_time:
                    if set_buy:
                        self.buy_max(price, time)
                        set_buy = None
                    else:
                        self.sell_max(price, time)
                        set_sell = None
                    transaction_time = None

            elif (
                price_diff >= buy_on_diff_percent_min
                and price_diff <= buy_on_diff_percent_max
                and set_buy is None
                and set_sell is None
                and self.avail_money // price
            ):
                set_buy = price
                transaction_time = time + delay_milis
            elif (
                price_diff >= sell_on_diff_percent_min
                and price_diff <= sell_on_diff_percent_max
                and set_buy is None
                and set_sell is None
                and self.current_stocks
            ):
                set_sell = price
                transaction_time = time + delay_milis

        self.sell_max(price, time)

        profit = self.avail_money - self.start_money
        return profit, self.transactions, self.total_time_invested
