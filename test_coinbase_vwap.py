""" Test for CoinbasePairsVWAP """
import json
import random
import unittest
import coinbase_vwap


def get_rounded(number):
    return  round(number, 3)


class TestCoinbaseVWAP(unittest.TestCase):
    """Test class for Coinbase calculating VWAP"""

    def get_sum_messages_param(self, param, min_index, index):
        """retun sum of item from messages"""
        return sum(
            item.get(param, 0) for item in self.fake_message[min_index:index]
        )

    def setUp(self):
        """Generate 250 fake messages and pre calculate VWAP for all of them"""
        self.cb_vwap = coinbase_vwap.CoinbasePairsVWAP()
        self.cb_vwap.multi_cpu_mode = False
        # generate fake data
        self.fake_message = [
            {
                'type': 'match',
                'product_id': coinbase_vwap.pairsType[0],
                'size': get_rounded(random.random() * 10),
                'price': get_rounded(random.random() * 100)
            } for i in range(250)
        ]

        # calculate vwap for each message
        min_index = 0
        for index, message in enumerate(self.fake_message):
            message['amount'] = float(message['price'] * message['size'])

            # if we got more than allowed max data points
            # didn't get old data
            if index >= coinbase_vwap.MAX_DATA_POINTS:
                min_index += 1

            total_amount = self.get_sum_messages_param(
                'amount', min_index, index+1
            )
            total_quantity = self.get_sum_messages_param(
                'size', min_index, index+1
            )
            message['vwap'] = total_amount / total_quantity

    def test_calc_vwap(self):
        """Check claulated vwap with class method"""
        for i in self.fake_message:
            self.cb_vwap.ws_message(None, json.dumps(i))
            self.assertEqual(get_rounded(i['vwap']),
                             get_rounded(self.cb_vwap.vwap))



if __name__ == "__main__":
    unittest.main()
