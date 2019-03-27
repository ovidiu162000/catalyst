import hmac
import json
from datetime import datetime
from decimal import Decimal

import ccxt
# from ccxt.base.decimal_to_precision import decimal_to_precision
# from ccxt.base.decimal_to_precision import DECIMAL_PLACES, TRUNCATE, ROUND
import crix
from crix.models import Resolution, NewOrder
from logbook import Logger
#from catalyst.constants import LOG_LEVEL
import requests

#log = Logger('crix', level=LOG_LEVEL)

class CrixClient(object):
    """
    HTTP client to the exchange for non-authorized requests.
    Supported environments:
    - 'mvp' - testnet sandbox with full-wipe each 2nd week (usually)
    - 'prod' - mainnet, production environment with real currency
    """
    env = 'mvp'
    enableRateLimit = False
    rateLimit = 2000  # milliseconds = seconds * 1000

    def __init__(self, api_key, api_secret, password):
        if self.env == 'prod':
            self._base_url = 'https://crix.io'
        else:
            self._base_url = 'https://{}.crix.io'.format(self.env)
        self._base_url += '/api/v1'
        
        self.client = crix.Client(env='mvp')
        self.key = api_key
        self.secret = api_secret
        self.auth_client = crix.AuthorizedClient(api_key, api_secret)

        #self.precisionMode = DECIMAL_PLACES
        self.substituteCommonCurrencyCodes = True
        self.commonCurrencies = {
            'XBT'   : 'BTC',
            'BCC'   : 'BCH',
            'DRK'   : 'DASH',
            'BCHABC': 'BCH',
            'BCHSV' : 'BSV',
        }

        self.name = 'Crix'
        self.markets = None
        self.tickers = None
        self.orders = {}
        self.timeframes = {
            '1m' : Resolution.one_minute,
            '5m' : Resolution.five_minutes,
            '15m': Resolution.fifteen_minutes,
            '30m': Resolution.half_an_hour,
            '1h' : Resolution.hour,
            '2h' : Resolution.two_hours,
            '4h' : Resolution.four_hours,
            '1d' : Resolution.day
        }

        self.has = {
            'CORS'                : False,
            'publicAPI'           : True,
            'privateAPI'          : True,
            'cancelOrder'         : True,
            'createDepositAddress': False,
            'createOrder'         : True,
            'deposit'             : False,
            'fetchBalance'        : True,
            'fetchClosedOrders'   : True,
            'fetchCurrencies'     : False,
            'fetchDepositAddress' : False,
            'fetchMarkets'        : True,
            'fetchMyTrades'       : True,
            'fetchOHLCV'          : True,
            'fetchOpenOrders'     : True,
            'fetchOrder'          : True,
            'fetchOrderBook'      : True,
            'fetchOrders'         : True,
            'fetchTicker'         : True,
            'fetchTickers'        : True,
            'fetchBidsAsks'       : False,
            'fetchTrades'         : False,
            'withdraw'            : False,
        }


    ############# Public methods
    def throttle(self):
        """
        Used if we have limits on requests
        """
        now = datetime.now().timestamp()
        elapsed = now - self.lastRestRequestTimestamp
        if elapsed < self.rateLimit:
            delay = self.rateLimit - elapsed
            time.sleep(delay / 1000.0)

    def load_markets(self):
        """
        Get dict of all symbols on the exchange.
        CCXT market structure:
        https://github.com/ccxt/ccxt/wiki/Manual#market-structure
        """
        markets = {}
        req = self.client.fetch_markets()

        for item in req:
            id = item.name
            base = item.base
            quote = item.quote
            symbol = base + '/' + quote
            active = item.is_trading
            precision = {
                'base': int(item.base_precision),
                'quote': int(item.quote_precision)
            }
            limits = {
                'amount': {
                    'min': float(item.min_lot),
                    'max': float(item.max_lot)
                },
                'price': {
                    'min': float(item.min_price),
                    'max': float(item.max_price)
                }
            }
            maker = float(item.maker_fee)
            taker = float(item.taker_fee)

            markets[symbol] = {
                'id': id,
                'symbol': symbol,
                'base': base,
                'quote': quote,
                'active': active,
                'precision': precision,
                'limits': limits,
                'maker': maker,
                'taker': taker,
                'info': item
            }
        self.markets = markets
        return markets

    def fetch_markets(self):
        """
        Returns all the markets on exchange in list format
        """
        ret = []
        if self.markets:
            ret = self.to_array(self.markets)
        else:
            ret = self.to_array(self.load_markets())
        return ret

    def common_currency_code(self, currency):
        return currency

    def fetch_ohlcv(self, symbol, timeframe='1m', since=None, limit=10):
        """
        Simulates the CCXT response for fetch_ohlcv
        """
        ret = []
        valid_limits = [1, 5, 10, 20, 50, 100, 500, 1000]
        if not self.markets:
            self.load_markets()
        
        ##### Validate parameters sent to crix_client #####
        req_symbol = self.ccxt_to_crix_symbol(symbol)
        req_timeframe = self.timeframes[timeframe]
        if limit not in valid_limits:
            print(
                "Exch[%s] fetch_ohlcv() got unsupported limit[%s]. "
                "Valid limits are: %s"
                % (self.name, limit, valid_limits)
            )
            return ret

        # utc_start_time is actually 'since' arg and has to be datetime obj
        if since is None:
            since = datetime(1980, 1, 1, 6, 0, 0)
        elif isinstance(since, (int, float)):
            try:
                since = datetime.fromtimestamp(since)
            except OSError as err:
                print(
                    "fetch_ohlcv() got arg since=%s which cannot be "
                    "converted to datetime object" % since
                )
                return
        
        # utc_end_time is not specified but will always be the current time
        utc_end_time = datetime.now()

        req = self.client.fetch_ohlcv(req_symbol, since, utc_end_time, req_timeframe, limit)

        for item in req:
            ts = item.open_time#.timestamp()
            open_price = float(item.open)
            high = float(item.high)
            low = float(item.low)
            close = float(item.close)
            vol = float(item.volume)

            ret.append(
                [ts, open_price, high, low, close, vol]
            )
        return ret

    def fetch_order_book(self, symbol, limit=10, params={}):
        """
        Orderbook as a dict
        """
        ret = {}
        req_symbol = self.ccxt_to_crix_symbol(symbol)

        req = self.client.fetch_order_book(req_symbol)#, level_aggregation=1)
        req_asks = req.asks
        req_bids = req.bids

        asks, bids  = [], []
        for item in req_asks:
            asks.append([float(item.price), float(item.quantity)])
        for item in req_bids:
            bids.append([float(item.price), float(item.quantity)])

        ts = datetime.now().timestamp() * 1000
        dt = self.ts_to_iso8601(ts)

        ret['bids'] = bids
        ret['asks'] = asks
        ret['timestamp'] = ts
        ret['datetime'] = dt
        return ret

    def fetch_tickers(self):
        """
        Get tickers for all symbols for the last 24 hours
        """
        tickers = []
        req = self.client.fetch_ticker()
        for item in req:
            symbol      = self.crix_to_ccxt_symbol(item.symbol_name)
            dt          = item.open_time
            timestamp   = dt.timestamp() * 1000
            open_price  = float(item.open)
            high        = float(item.high)
            low         = float(item.low)
            close       = float(item.close)
            last        = close
            volume      = float(item.volume)        #TODO figure out if this is the baseVolume or quoteVolume
            prev_close  = float(item.prev_close_price)
            change      = float(item.price_change)
            percentage  = float(item.price_change_percent)
            average     = (last + open_price) / 2

            ticker = {
                'symbol'        : symbol,
                'info'          : item,
                'timestamp'     : timestamp,
                'datetime'      : dt,
                'high'          : high,
                'low'           : low,
                'open'          : open_price,
                'close'         : close,
                'last'          : last,
                'previousClose' : prev_close,
                'change'        : change,
                'percentage'    : percentage,
                'average'       : average,
                'baseVolume'    : volume
            }
            tickers.append(ticker)
        self.tickers = tickers
        return tickers

    def fetch_ticker(self, symbol):
        """
        Get ticker for a specific symbol
        """
        ret = {}
        if self.tickers is None:
            self.fetch_tickers()

        for ticker in self.tickers:
            if ticker['symbol'] == symbol:
                ret = ticker
                break
        return ret

    def common_currency_code(self, currency):
        if not self.substituteCommonCurrencyCodes:
            return currency
        
        if (
                currency is not None
            and (currency in self.commonCurrencies)
            and self.commonCurrencies[currency] is not None
        ):
            return str(self.commonCurrencies[currency])
        else:
            return currency

    #TODO implement amount_to_precision
    def amount_to_precision(self, symbol, amount):
        #return self.decimal_to_precision(amount, TRUNCATE, self.markets[symbol]['precision']['amount'], self.precisionMode)
        pass

    ############# Authenticated methods
    def fetch_balance(self):
        """
        Balance as a dict
        """
        ret = {}
        resp = self.auth_client.fetch_balance()

        bal_free = {}
        bal_used = {}
        bal_total = {}
        for item in resp:
            coin  = item.currency_name
            total = float(item.balance)
            used  = float(item.locked_balance)
            free  = total - used

            bal_free[coin]  = free
            bal_used[coin]  = used
            bal_total[coin] = total

            ret[coin] = {
                'free' : free,
                'used' : used,
                'total': total
            }
        
        ret['free']  = bal_free
        ret['used']  = bal_used
        ret['total'] = bal_total
        ret['info']  = resp

        return ret

    def create_order(self, symbol, type, side, amount, price=None, params={}):
        """
        symbol: str
        price: Decimal
        quantity: Decimal
        is_buy: bool
        time_in_force: TimeInForce = TimeInForce.good_till_cancel
        stop_price: Optional[Decimal] = None
        expire_time: Optional[datetime] = None
        """
        req_symbol = self.ccxt_to_crix_symbol(symbol)
        if side.upper() == "BUY":
            is_buy = True
        else:
            is_buy = False
        req_amount = Decimal(amount)
        req_price = Decimal(price)

        time_in_force = params.get('time_in_force', TimeInForce.good_till_cancel)
        stop_price = params.get('stop_price', None)
        expire_time = params.get('expire_time', None)

        order = NewOrder(
            symbol=req_symbol,
            price=req_price,
            quantity=req_amount,
            is_buy=is_buy,
            time_in_force=time_in_force,
            stop_price=stop_price,
            expire_time=expire_time
        )

        resp = self.auth_client.create_order(order)
        ret = {
            'id': str(resp.id),
            'info': resp
        }
        self.orders[ret['id']] = ret
        return ret

    def cancel_order(self, order_id, symbol, params={}):
        """
        Cancel placed order
        """
        canceled_order = None
        if isinstance(order_id, str):
            try:
                order_id = int(order_id)
            except ValueError:
                print(
                    "Exch[%s] cancel_order() got string order_id=%s. "
                    "Can't convert to int."
                    % (self.name, order_id)
                )
        req_symbol = self.ccxt_to_crix_symbol(symbol)
        req = self.auth_client.cancel_order(order_id, req_symbol)
        canceled_order = self.parse_order(req)
        if canceled_order['id'] in self.orders:
            del self.orders[canceled_order['id']]
        return canceled_order

    def fetch_open_orders(self, symbol=None, since=None, limit=int(1000), params={}):
        """
        Get all open orders for the user.
        One request per each symbol will be made plus additional
        request to query all supported symbols if symbols parameter
        not specified.
        """
        #TODO implement since
        orders = []
        symbols = symbol
        if isinstance(symbol, str):
            symbol = self.ccxt_to_crix_symbol(symbol)
            symbols = [symbol]
        if symbol is None:
            ccxt_symbols = self.fetch_markets()
            symbols = [self.ccxt_to_crix_symbol(symbol) for symbol in ccxt_symbols]

        resp = self.auth_client.fetch_open_orders(*symbols, limit=limit)
        req_orders = [order for order in resp]
        print("fetch_open_orders got %s req_orders" % len(req_orders))
        for order in req_orders:
            parsed_order = self.parse_order(order)
            orders.append(parsed_order)
            if parsed_order['id'] not in self.orders:
                self.orders[parsed_order['id']] = parsed_order
        return orders

    def fetch_closed_orders(self, symbol=None, since=None, limit=int(1000), params={}):
        """
        Get complete (filled, canceled) orders for user
        One request per each symbol will be made plus additional
        request to query all supported symbols if symbols parameter
        not specified.
        """
        #TODO doesnt seem to work, always returns empty
        #TODO implement since
        orders = []
        symbols = symbol
        if isinstance(symbol, str):
            symbol = self.ccxt_to_crix_symbol(symbol)
            symbols = [symbol]
        if symbol is None:
            ccxt_symbols = self.fetch_markets()
            symbols = [self.ccxt_to_crix_symbol(symbol) for symbol in ccxt_symbols]

        resp = self.auth_client.fetch_closed_orders(*symbols, limit=limit)
        req_orders = [order for order in resp]
        print("fetch_closed_orders got %s req_orders" % len(req_orders))
        for order in req_orders:
            parsed_order = self.parse_order(order)
            orders.append(parsed_order)
        return orders

    def fetch_order(self, order_id, symbol=None):
        """
        Fetch single open order info
        """
        if isinstance(order_id, str):
            try:
                order_id = int(order_id)
            except ValueError:
                print(
                    "Exch[%s] cancel_order() got string order_id=%s. "
                    "Can't convert to int."
                    % (self.name, order_id)
                )
        req_symbol = self.ccxt_to_crix_symbol(symbol)
        req = self.auth_client.fetch_order(order_id, req_symbol)
        return self.parse_order(req)

    def fetch_orders(self, symbol=None, since=None, limit=int(1000), params={}):
        """
        Get opened and closed orders filtered by symbols. If no symbols specified - all symbols are used.
        Basically the function acts as union of fetch_open_orders and fetch_closed_orders.
        """
        #TODO implement since
        orders = []
        symbols = symbol
        if isinstance(symbol, str):
            symbol = self.ccxt_to_crix_symbol(symbol)
            symbols = [symbol]
        if symbol is None:
            ccxt_symbols = self.fetch_markets()
            symbols = [self.ccxt_to_crix_symbol(symbol) for symbol in ccxt_symbols]

        resp = self.auth_client.fetch_orders(*symbols, limit=limit)
        req_orders = [order for order in resp]
        print("fetch_orders() got %s req_orders" % len(req_orders))
        for order in req_orders:
            parsed_order = self.parse_order(order)
            orders.append(parsed_order)
        return orders

    def fetch_my_trades(self, symbol=None, since=None, limit=int(1000), params={}):
        """
        Get all trades for the user. There is some gap (a few ms)
        between time when trade is actually created and time
        when it becomes visible for the user.
        """
        # TODO not sure what happens here, it appears that we're getting orders, not actual filled trades
        #TODO implement since
        trades = []
        symbols = symbol
        if isinstance(symbol, str):
            symbol = self.ccxt_to_crix_symbol(symbol)
            symbols = [symbol]
        if symbol is None:
            ccxt_symbols = self.fetch_markets()
            symbols = [self.ccxt_to_crix_symbol(symbol) for symbol in ccxt_symbols]

        resp = self.auth_client.fetch_my_trades(*symbols, limit=limit)
        req_trades = [order for order in resp]
        print("fetch_my_trades got %s req_trades" % len(req_trades))
        for trade in req_trades:
            trade_id = str(trade.id)
            ts = trade.created_at.timestamp() * 1000
            dt = self.ts_to_iso8601(ts)
            symbol = self.crix_to_ccxt_symbol(trade.symbol_name)
            side = 'buy' if trade.is_buy else 'sell'
            price = float(trade.price)
            amount = float(trade.quantity)
            cost = 0
            if side == 'buy':
                if trade.is_buy_order_filled:
                    cost = amount * price
            if side == 'sell':
                if trade.is_sell_order_filled:
                    cost = amount * price

            ret = {
                'info': trade,
                'id': trade_id,
                'timestamp': ts,
                'datetime': dt,
                'symbol': symbol,
                'order': trade_id,
                'type': 'limit',
                'side': side,
                'takerOrMaker': None,
                'price': price,
                'amount': amount,
                'cost': cost,
                'fee': {}
            }
            trades.append(ret)
        return trades

    def parse_order(self, order):
        ord_id = str(order.id)
        status = order.status
        if status.value == 0:
            ord_status = 'open'
        elif status.value == 1:
            ord_status = 'closed'
        elif status.value == 2:
            ord_status = 'canceled'
        symbol = self.crix_to_ccxt_symbol(order.symbol_name)
        side = 'buy' if order.is_buy else 'sell'
        price = float(order.price)
        amount = float(order.quantity)
        filled = float(order.filled_quantity)
        remaining = amount - filled
        cost = filled * price

        ret = {
            'id'                : ord_id,
            'datetime'          : None,
            'timestamp'         : None,
            'lastTradeTimestamp': None,
            'status'            : ord_status,
            'symbol'            : symbol,
            'type'              : 'limit',
            'side'              : side,
            'price'             : price,
            'amount'            : amount,
            'filled'            : filled,
            'remaining'         : remaining,
            'trades'            : [],
            'fee'               : {},
            'info'              : order
        }
        return ret

    @staticmethod
    def to_array(value):
        return list(value.keys()) if type(value) is dict else value

    @staticmethod
    def ccxt_to_crix_symbol(symbol):
        return symbol.replace('/', '_')

    @staticmethod
    def crix_to_ccxt_symbol(symbol):
        return symbol.replace('_', '/')

    @staticmethod
    def ts_to_iso8601(timestamp):
        now = datetime.fromtimestamp(timestamp // 1000)
        return now.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-6] + "{:03d}".format(int(timestamp) % 1000) + 'Z'


def test_print(method, msg):
    print(10*'#', ' START ', method, ' ', 10*'#')
    print(msg)
    print(10*'#', ' END ', method, ' ', 10*'#')
    print('\n')

key = ""
secret = ""
password = "test"
symbol = 'ETH/BTC'
ord_type = 'limit'
side = 'sell'
amount = 0.1
price = 0.1


crix_client = CrixClient(key, secret, password)
markets = crix_client.load_markets()
test_print('load_markets()', markets)

fetched_markets = crix_client.fetch_markets()
test_print('fetch_markets()', fetched_markets)

fetched_tickers = crix_client.fetch_tickers()
test_print('fetch_tickers()', fetched_tickers)

bal = crix_client.fetch_balance()
test_print('fetch_balance()', bal)

closed_orders = crix_client.fetch_closed_orders()
test_print('fetch_closed_orders()', closed_orders)

open_orders = crix_client.fetch_open_orders()
test_print('fetch_open_orders()', open_orders)

orders = crix_client.fetch_orders()
test_print('fetch_orders()', orders)

my_trades = crix_client.fetch_my_trades()
test_print('fetch_my_trades()', my_trades)