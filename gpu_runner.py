from numba import cuda


@cuda.jit
def run_strat(strategies, data):
    thread_position = cuda.grid(1)
    buy_on_diff_percent = strategies[thread_position][0]
    sell_on_diff_percent = strategies[thread_position][1]
    range_b = strategies[thread_position][2]
    range_s = strategies[thread_position][3]

    buy_on_diff_percent_min = buy_on_diff_percent - (range_b)
    buy_on_diff_percent_max = buy_on_diff_percent + (range_b)
    sell_on_diff_percent_min = sell_on_diff_percent - (range_s)
    sell_on_diff_percent_max = sell_on_diff_percent + (range_s)
    aggregate = strategies[thread_position][4]
    start_money = 100000
    avail_money = start_money
    current_stocks = 0
    transactions = 0
    last_buy_time = 0
    total_time_invested = 0
    delay_milis = 5000

    item_list=[]
    set_buy = None
    set_sell = None
    transaction_time = None
    for item in data:
        item_list.append(item)
        time = item[0]
        price = item[1]
        
        old_items_index=None
        for i in range(len(item_list)):
            if i[0] + aggregate < item[0]:
                old_items_index = i
            else:
                break
        if old_items_index is not None:
            if old_items_index+1 == len(item_list):
                #Keeps at least 1 object
                old_items_index = old_items_index - 1
            item_list=item_list[old_items_index+1:]
        previous_price = item_list[0][1]       

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
