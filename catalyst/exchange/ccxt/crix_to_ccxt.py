from decimal import Decimal
from datetime import datetime, timedelta
import hmac
import json
import re
import time

#from catalyst.constants import LOG_LEVEL
import ccxt
from ccxt.base.decimal_to_precision import decimal_to_precision
from ccxt.base.decimal_to_precision import DECIMAL_PLACES, TRUNCATE, ROUND
import crix
from crix.models import Resolution, NewOrder
from logbook import Logger
import requests

#log = Logger('crix', level=LOG_LEVEL)

class CrixClient(object):
    """
    HTTP client to the exchange for non-authorized requests.
    Supported environments:
    - 'mvp' - testnet sandbox with full-wipe each 2nd week (usually)
    - 'prod' - mainnet, production environment with real currency
    """

    def __init__(self, api_key, api_secret, password, env='mvp'):
        self.env = env
        if self.env == 'prod':
            self._base_url = 'https://crix.io'
        else:
            self._base_url = 'https://{}.crix.io'.format(self.env)
        self._base_url += '/api/v1'
        
        self.client = crix.Client(env='mvp')
        self.key = api_key
        self.secret = api_secret
        self.auth_client = crix.AuthorizedClient(api_key, api_secret)

        # Currently for BOT API there is a rate limit about 100 requests/second
        self.enableRateLimit = True
        self.lastRestRequestTimestamp = 0
        self.rateLimit = 1000

        self.precisionMode = DECIMAL_PLACES
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
        if self.enableRateLimit:
            self.throttle()
        req = self.client.fetch_markets()
        self.lastRestRequestTimestamp = datetime.now().timestamp()

        for item in req:
            id = item.name
            base = item.base
            quote = item.quote
            symbol = base + '/' + quote
            active = item.is_trading
            precision = {
                'base': int(item.base_precision),
                'quote': int(item.quote_precision),
                'amount': self.precision_from_string(str(float(item.tick_lot))),
                'price': self.precision_from_string(str(float(item.tick_price)))
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
        # utc_end_time is not specified but will always be the current time
        utc_end_time = datetime.now()
        if since is None:
            since = self.generate_ohlcv_start_time(utc_end_time, timeframe, since, limit)
        elif isinstance(since, (int, float)):
            try:
                since = datetime.fromtimestamp(since)
            except OSError as err:
                since = datetime.fromtimestamp(since/1000)
            except OSError as err:
                print(
                    "fetch_ohlcv() got arg since=%s which cannot be "
                    "converted to datetime object" % since
                )
                return ret

        if self.enableRateLimit:
            self.throttle()
        req = self.client.fetch_ohlcv(req_symbol, since, utc_end_time, req_timeframe, limit)
        self.lastRestRequestTimestamp = datetime.now().timestamp()

        for item in req:
            ts = item.open_time.timestamp() * 1000
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

        if self.enableRateLimit:
            self.throttle()
        req = self.client.fetch_order_book(req_symbol)#, level_aggregation=1)
        self.lastRestRequestTimestamp = datetime.now().timestamp()
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

        if self.enableRateLimit:
            self.throttle()
        req = self.client.fetch_ticker()
        self.lastRestRequestTimestamp = datetime.now().timestamp()

        for item in req:
            symbol      = self.crix_to_ccxt_symbol(item.symbol_name)
            dt          = item.open_time
            timestamp   = dt.timestamp() * 1000
            open_price  = float(item.open)
            high        = float(item.high)
            low         = float(item.low)
            close       = float(item.close)
            last        = close
            volume      = float(item.volume)
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

    def amount_to_precision(self, symbol, amount):
        return decimal_to_precision(amount, TRUNCATE, self.markets[symbol]['precision']['amount'], self.precisionMode)


    ############# Authenticated methods
    def fetch_balance(self):
        """
        Balance as a dict
        """
        ret = {}

        if self.enableRateLimit:
            self.throttle()
        resp = self.auth_client.fetch_balance()
        self.lastRestRequestTimestamp = datetime.now().timestamp()

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

        if self.enableRateLimit:
            self.throttle()
        resp = self.auth_client.create_order(order)
        self.lastRestRequestTimestamp = datetime.now().timestamp()

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

        if self.enableRateLimit:
            self.throttle()
        req = self.auth_client.cancel_order(order_id, req_symbol)
        self.lastRestRequestTimestamp = datetime.now().timestamp()

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
        orders = []
        symbols = symbol
        if isinstance(symbol, str):
            symbol = self.ccxt_to_crix_symbol(symbol)
            symbols = [symbol]
        if symbol is None:
            ccxt_symbols = self.fetch_markets()
            symbols = [self.ccxt_to_crix_symbol(symbol) for symbol in ccxt_symbols]

        if self.enableRateLimit:
            self.throttle()
        resp = self.auth_client.fetch_open_orders(*symbols, limit=limit)
        self.lastRestRequestTimestamp = datetime.now().timestamp()
        
        req_orders = [order for order in resp]
        for order in req_orders:
            parsed_order = self.parse_order(order)
            orders.append(parsed_order)
            if parsed_order['id'] not in self.orders:
                self.orders[parsed_order['id']] = parsed_order
        if since is not None:
            orders = self.filter_array_by_since(orders, since)
        return orders

    def fetch_closed_orders(self, symbol=None, since=None, limit=int(1000), params={}):
        """
        Get complete (filled, canceled) orders for user
        One request per each symbol will be made plus additional
        request to query all supported symbols if symbols parameter
        not specified.
        """
        orders = []
        symbols = symbol
        if isinstance(symbol, str):
            symbol = self.ccxt_to_crix_symbol(symbol)
            symbols = [symbol]
        if symbol is None:
            ccxt_symbols = self.fetch_markets()
            symbols = [self.ccxt_to_crix_symbol(symbol) for symbol in ccxt_symbols]

        if self.enableRateLimit:
            self.throttle()
        resp = self.auth_client.fetch_closed_orders(*symbols, limit=limit)
        self.lastRestRequestTimestamp = datetime.now().timestamp()

        req_orders = [order for order in resp]
        for order in req_orders:
            parsed_order = self.parse_order(order)
            orders.append(parsed_order)
        if since is not None:
            orders = self.filter_array_by_since(orders, since)
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

        if self.enableRateLimit:
            self.throttle()
        req = self.auth_client.fetch_order(order_id, req_symbol)
        self.lastRestRequestTimestamp = datetime.now().timestamp()
        return self.parse_order(req)

    def fetch_orders(self, symbol=None, since=None, limit=int(1000), params={}):
        """
        Get opened and closed orders filtered by symbols. If no symbols specified - all symbols are used.
        Basically the function acts as union of fetch_open_orders and fetch_closed_orders.
        """
        orders = []
        symbols = symbol
        if isinstance(symbol, str):
            symbol = self.ccxt_to_crix_symbol(symbol)
            symbols = [symbol]
        if symbol is None:
            ccxt_symbols = self.fetch_markets()
            symbols = [self.ccxt_to_crix_symbol(symbol) for symbol in ccxt_symbols]

        if self.enableRateLimit:
            self.throttle()
        resp = self.auth_client.fetch_orders(*symbols, limit=limit)
        self.lastRestRequestTimestamp = datetime.now().timestamp()

        req_orders = [order for order in resp]

        for order in req_orders:
            parsed_order = self.parse_order(order)
            orders.append(parsed_order)

        if since is not None:
            orders = self.filter_array_by_since(orders, since)
        return orders

    def fetch_my_trades(self, symbol=None, since=None, limit=int(1000), params={}):
        """
        Get all trades for the user. There is some gap (a few ms)
        between time when trade is actually created and time
        when it becomes visible for the user.
        """
        trades = []
        symbols = symbol
        if isinstance(symbol, str):
            symbol = self.ccxt_to_crix_symbol(symbol)
            symbols = [symbol]
        if symbol is None:
            ccxt_symbols = self.fetch_markets()
            symbols = [self.ccxt_to_crix_symbol(symbol) for symbol in ccxt_symbols]

        if self.enableRateLimit:
            self.throttle()
        resp = self.auth_client.fetch_my_trades(*symbols, limit=limit)
        self.lastRestRequestTimestamp = datetime.now().timestamp()

        req_trades = [order for order in resp]
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

        if since is not None:
            trades = self.filter_array_by_since(trades, since)
        return trades


    ############# Helper methods
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
    def filter_array_by_since(array, since):
        ret = []
        for item in array:
            timestamp = item.get('timestamp', None)
            if timestamp:
                if timestamp >= since:
                    ret.append(item)
            else:
                ret.append(item)
        return ret

    def generate_ohlcv_start_time(self, now, timeframe, since, limit):
        """
        crix requires to input an utc_start_time when requesting OHLCV
        this start_time can't be fixed, because it depends in timeframe
        returns start_time = current time - (timeframe * limit)
        return is datetime object
        """
        start_time = 0
        if timeframe == '1m':
            total_minutes = limit
        elif timeframe == '5m':
            total_minutes = limit * 5
        elif timeframe == '15m':
            total_minutes = limit * 15
        elif timeframe == '30m':
            total_minutes = limit * 30
        elif timeframe == '1h':
            total_minutes = limit * 60
        elif timeframe == '2h':
            total_minutes = limit * 60 * 2
        elif timeframe == '4h':
            total_minutes = limit * 60 * 4
        elif timeframe == '1d':
            total_minutes = limit * 60 * 24

        days, hours, minutes = self.get_min_hr_day(total_minutes)
        delta_to_sustract = timedelta(days=days, hours=hours, minutes=minutes)

        return now - delta_to_sustract

    @staticmethod
    def get_min_hr_day(total_time):
        hours = total_time // 60
        days = hours // 24
        minutes = total_time % 60
        return days, hours, minutes

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

    @staticmethod
    def precision_from_string(string):
        parts = re.sub(r'0+$', '', string).split('.')
        return len(parts[1]) if len(parts) > 1 else 0