from numba import cuda


@cuda.jit
def run_strat(strategies, data):
    thread_position = cuda.grid(1)
    buy_on_diff_percent = strategies[thread_position][0]
    sell_on_diff_percent = strategies[thread_position][1]
    range_b = strategies[thread_position][2]
    range_s = strategies[thread_position][3]

    buy_on_diff_percent_min = buy_on_diff_percent - (range_b / 2)
    buy_on_diff_percent_max = buy_on_diff_percent + (range_b / 2)
    sell_on_diff_percent_min = sell_on_diff_percent - (range_s / 2)
    sell_on_diff_percent_max = sell_on_diff_percent + (range_s / 2)
    start_money = 100000
    avail_money = start_money
    current_stocks = 0
    transactions = 0
    last_buy_time = 0
    total_time_invested = 0
    delay_milis = 5000

    previous_price = None
    set_buy = None
    set_sell = None
    transaction_time = None
    for item in data:
        price = previous_price
        time = item[0]
        price = item[1]

        if previous_price is None:
            previous_price = price
        price_diff = ((price / previous_price) - 1) * 100
        if transaction_time is not None:
            if time > transaction_time:
                if set_buy is not None:
                    buying_amount = avail_money // price
                    if buying_amount:
                        transactions = transactions + 1
                        last_buy_time = time
                        current_stocks = current_stocks + buying_amount
                        avail_money = avail_money - price * buying_amount
                    set_buy = None
                else:
                    if current_stocks != 0:
                        transactions = transactions + 1
                        total_time_invested = total_time_invested + time - last_buy_time
                        avail_money = avail_money + price * current_stocks
                        current_stocks = 0
                    set_sell = None
                transaction_time = None

        elif (
            price_diff >= buy_on_diff_percent_min
            and price_diff <= buy_on_diff_percent_max
            and set_buy is None
            and set_sell is None
            and avail_money // price
        ):
            set_buy = price
            transaction_time = time + delay_milis
        elif (
            price_diff >= sell_on_diff_percent_min
            and price_diff <= sell_on_diff_percent_max
            and set_buy is None
            and set_sell is None
            and current_stocks
        ):
            set_sell = price
            transaction_time = time + delay_milis

    if current_stocks:
        transactions = transactions + 1
        total_time_invested = total_time_invested + time - last_buy_time
        avail_money = avail_money + price * current_stocks
        current_stocks = 0

    profit = avail_money - start_money

    strategies[thread_position][4] = profit
    strategies[thread_position][5] = transactions
    strategies[thread_position][6] = total_time_invested
