
class Runner():
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
        
        buying_amount = self.avail_money//price
        if buying_amount:
            self.transactions = self.transactions + 1
            self.last_buy_time = time
            #print(f"buying {buying_amount} at {price}")
            self.current_stocks = self.current_stocks + buying_amount
            self.avail_money = self.avail_money - price*buying_amount

    def sell_max(self, price, time):
        if self.current_stocks:
            self.transactions = self.transactions + 1
            self.total_time_invested = self.total_time_invested + time - self.last_buy_time
            #print(f"selling {self.current_stocks} at {price}")
            self.avail_money = self.avail_money + price*self.current_stocks
            self.current_stocks = 0

    def run_strat(self, buy_on_diff_percent_min, buy_on_diff_percent_max, sell_on_diff_percent_min, sell_on_diff_percent_max, delay_milis=5000, variable=None):
        previous_price = None
        #print("start")
        set_buy = None
        set_sell = None
        transaction_time = None
        for item in self.data:
            price=previous_price
            time = item[0]
            price = item[1]

            if previous_price is None:
                previous_price = price
            price_diff = ((price/previous_price)-1)*100
            if transaction_time is not None:
                if time > transaction_time:
                    #print(f"transaction at {time}")
                    if set_buy:
                        self.buy_max(price, time)
                        set_buy = None
                    else:
                        self.sell_max(price, time)
                        set_sell = None
                    transaction_time = None
            
            elif price_diff >= buy_on_diff_percent_min and price_diff <= buy_on_diff_percent_max and set_buy is None and set_sell is None and self.avail_money//price:
                set_buy = price
                transaction_time = time+delay_milis
                #print(f"{time} set buy at {transaction_time}")
            elif price_diff >= sell_on_diff_percent_min and price_diff <= sell_on_diff_percent_max and set_buy is None and set_sell is None and self.current_stocks:
                set_sell = price
                transaction_time = time+delay_milis
                #print(f"{time} set sell at {transaction_time}")


        #print("final sell")
        self.sell_max(price, time)

        profit = self.avail_money-self.start_money
        #profit_percent = ((self.avail_money/self.start_money)-1)*100
        #print(f"profit {profit_percent} for {buy_on_diff_percent} and {sell_on_diff_percent}")
        #self.profit_percent = profit_percent
        #return self.profit_percent

        return profit, self.transactions, self.total_time_invested