# HK Open Paper Trader

Runs by itself on GitHub Actions every weekday. Places **no orders**, uses **no API keys**
(only public Binance prices).

Each weekday at 01:02 UTC (06:02 AM Pakistan) it checks 5 Hong Kong / China stock futures against
the previous weekday's 08:00 UTC price. If a price is 1% or more above it, it records a paper short,
then records the real order-book exit at 01:32 and 02:00 UTC.

Results (updated automatically):
- `data/summary.md`  progress, averages and a plain verdict
- `data/trades.csv`  every trade with full details (send this file or this repo link to Claude)
- `data/sessions/`   one raw file per day
- `data/check.txt`   last connection test

First-time test: **Actions tab -> "HK open paper trader" -> Run workflow -> mode: check**.
The rule is frozen. Do not edit `paper_trader.py` while the test is running.
