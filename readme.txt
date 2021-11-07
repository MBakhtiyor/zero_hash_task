# Calc VWAP using Coinbase websocket for 3 pairs

Run script
`python coinbase_vwap.py`

Run unittest
`python -m unittest test_coinbase_vwap.py`

Design of solution:
1. If CPU has more than 1 core:
- Run receive message from websocket and calculation VWAP in different CPU cores
- `ws_message` put each received message to Queue
- `calc_vwap` Read message from queue and start calculating VWAP
- `pairs_trade` is a dict which store last 200 pairs data and pre-calculated data for total_amount and quantity. It allow us skip calculating on each iteration throw all 200 items

2. If CPU has 1 core:
- Run receive message from websocket
- `ws_message` call `calc_vwap` with new parameter from websocket
- `calc_vwap` calculating VWAP
- `pairs_trade` is a dict which store last 200 pairs data and pre-calculated data for total_amount and quantity. It allow us skip calculating on each iteration throw all 200 items


# TODO
- Add more unittest for checking socket connections
