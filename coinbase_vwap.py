"""Get vwap price for pairs from coinbase.com"""

from enum import Enum
from multiprocessing import cpu_count, Manager
from collections import deque

import traceback
import json
import concurrent.futures
import logging
import websocket


logging.basicConfig(
    filename='coinbase_vwap.log',
    filemode='w',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


NUM_CORES = cpu_count() # Our number of CPU cores
MAX_WORKERS = 2

#  Coinbase parameters
MAX_DATA_POINTS = 200
pairsType = [
    'BTC-USD',
    'ETH-USD',
    'ETH-BTC'
]
COIN_BASE_WEB_SOCKET_URL = 'wss://ws-feed.exchange.coinbase.com'
SUBSCRIPTION_FULL = json.dumps({
    'type': 'subscribe',
    'product_ids': pairsType,
    'channels': [
        'full'
    ]
})


class Trade(Enum):
    """Trade enum types"""
    SUM = 'trade_sum'
    QNT = 'trade_qnt'
    TOTAL_SUM = 'total_trade_sum'
    TOTAL_QNT = 'total_trade_qnt'


class CoinbasePairsVWAP:
    """ Calculating VWAP for pairsType """
    def __init__(self):
        # total sum and total quantity (size) of trades
        self.pairs_trade = {
            Trade.SUM: {},
            Trade.QNT: {},
            Trade.TOTAL_SUM: {},
            Trade.TOTAL_QNT: {}
        }
        self.multi_cpu_mode = True  # control multi cpu run
        self.sum_price_of_trade = 0
        self.sum_quantity_of_trade = 0
        self.trading_pair_queue = Manager().Queue()  # store all data from socket
        self.vwap = 0
        for pair_type in pairsType:
            self.pairs_trade[Trade.SUM][pair_type] = deque()  # store data points amount of trade
            self.pairs_trade[Trade.QNT][pair_type] = deque()  # store data points qnt of trade
            self.pairs_trade[Trade.TOTAL_SUM][pair_type] = 0  # total amount of trades
            self.pairs_trade[Trade.TOTAL_QNT][pair_type] = 0  # total qnt if trades

    def __remove_old_trade_data(self, pair_type: str, trade_type: str,
                                trade_total_type: str):
        """ Remove old trade data and decrease total_sum"""
        pop_sum = self.pairs_trade[trade_type][pair_type].popleft()
        self.pairs_trade[trade_total_type][pair_type] -= pop_sum

    def calc_vwap(self, trade_msg=None):
        """Calculating VWAP"""
        while True:
            try:
                # get message from parameter global trading pair Queue
                message = trade_msg or self.trading_pair_queue.get()
                pair_type = message['product_id']
                process_items_len = len(self.pairs_trade[Trade.SUM][pair_type])

                # check for maximum data point
                if process_items_len == MAX_DATA_POINTS:
                    # Remove old trading pair
                    self.__remove_old_trade_data(
                        pair_type, Trade.SUM, Trade.TOTAL_SUM)
                    self.__remove_old_trade_data(
                        pair_type, Trade.QNT, Trade.TOTAL_QNT)

                # add new trade info to global dict
                trade_size = float(message['size'])
                trade_sum = float(message['price']) * trade_size
                self.pairs_trade[Trade.SUM][pair_type].append(trade_sum)
                self.pairs_trade[Trade.QNT][pair_type].append(trade_size)

                # increase total amount and quantity of trades
                self.pairs_trade[Trade.TOTAL_SUM][pair_type] += trade_sum
                self.pairs_trade[Trade.TOTAL_QNT][pair_type] += trade_size

                # calculate vwap
                self.vwap = (
                    self.pairs_trade[Trade.TOTAL_SUM][pair_type] /
                        (self.pairs_trade[Trade.TOTAL_QNT][pair_type] or 1)
                )
                print(pair_type, ' -> ', self.vwap)

                if not self.multi_cpu_mode:
                    break

            except Exception:
                logging.exception(traceback.format_exc())

    def ws_message(self, ws, message):
        """WebSocket callback functions"""
        try:
            message = json.loads(message)
            if message.get('type', '') == 'match':
                if self.multi_cpu_mode:
                    self.trading_pair_queue.put(message)
                else:
                    self.calc_vwap(message)

        except Exception:
            logging.exception(traceback.format_exc())

    def ws_open(self, ws):
        """Websocket open, send subscription to our pairs"""
        ws.send(SUBSCRIPTION_FULL)

    def ws_connect(self):
        """Websocket connect"""
        ws = websocket.WebSocketApp(
            COIN_BASE_WEB_SOCKET_URL,
            on_open = self.ws_open,
            on_message = self.ws_message)
        ws.run_forever()


if __name__ == '__main__':

    coinbase_vwap = CoinbasePairsVWAP()
    if NUM_CORES < MAX_WORKERS:
        # run in single cpu mode
        coinbase_vwap.multi_cpu_mode = False
        coinbase_vwap.ws_connect()
    else:
        #  run on multi cpu mode
        tasks = []
        with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            tasks.append(executor.submit(coinbase_vwap.ws_connect))  # add to queue
            tasks.append(executor.submit(coinbase_vwap.calc_vwap))  # get from queue

            concurrent.futures.wait(tasks)
