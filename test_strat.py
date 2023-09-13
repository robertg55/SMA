from datetime import datetime, timedelta
from bruteforce_strats import poll_yfinnance, run_single


def test_strat(symbol, b, s, days):
    data = []
    old_date = datetime.now() - timedelta(days=days)
    month = [
        old_date + timedelta(idx + 1) for idx in range((datetime.now() - old_date).days)
    ]
    for day in month:
        data.extend(
            poll_yfinnance(
                symbol,
                start_time=day.strftime("%Y-%m-%d"),
                end_time=(day + timedelta(1)).strftime("%Y-%m-%d"),
            )
        )
    result = run_single(data, b, s)
    _, _, profit, transactionsnb, itime = result.split("_")
    print(
        f"total profit {profit}, total transaction {transactionsnb}, total invested time hours {float(itime)/60/60}"
    )
