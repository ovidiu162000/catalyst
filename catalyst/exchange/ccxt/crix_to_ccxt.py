import hmac
import json
from datetime import datetime
from decimal import Decimal

import ccxt
import crix
from crix.models import Ticker, Resolution, NewOrder, Order, Symbol, Depth, Trade, Account, Ticker24, TimeInForce
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
        
        self.client      = crix.Client(env='mvp')
        self.key = api_key
        self.secret = api_secret
        self.auth_client = crix.AuthorizedClient(api_key, api_secret)

        self.name = 'Crix'
        self.markets = None
        self.tickers = None
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
            maker = item.maker_fee
            taker = item.taker_fee

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
            ret = self.to_arrya(self.load_markets())
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
        TODO: skipping because seems to not work
        """
        req_symbol = self.ccxt_to_crix_symbol(symbol)

        req = self.client.fetch_order_book(req_symbol, limit)

        print(req, type(req))
        # params = {
        #     'symbolName': req_symbol
        # }
        # if limit is not None:
        #     params['levelAggregation'] = limit
        # req2 = requests.post(self._base_url + '/depths', json={
        #     'req': params
        # })
        # print(req2)
        # response = req2.json()
        # print(type(response), response)

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

    def create_order(
            self, 
            symbol, 
            type, 
            side, 
            amount, 
            price=None, 
            params={}
        ):
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
        return ret

    def fetch_open_orders(self, symbol=None, since=None, limit=int(10), params={}):
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
            symbols = []
        print(symbols)
        
        # resp = self.auth_client.fetch_open_orders(symbols, limit)
        # orders = [order for order in resp]
        # print(orders)

        response = self.signed_request('fetch-my-trades', self._base_url + '/user/trades', {
            'req': {
                'limit': limit,
                'symbolName': symbols
            }
        })
        print(type(response), response)

        return orders


    def signed_request(self, operation, url, json_data):
        payload = json.dumps(json_data).encode()
        signer = hmac.new(self.secret.encode(), digestmod='SHA256')
        signer.update(payload)
        signature = signer.hexdigest()
        headers = {
            'X-Api-Signed-Token': self.key + ',' + signature,
        }
        req = requests.post(url, data=payload, headers=headers)
        #APIError.ensure(operation, req)
        return req.json()

    @staticmethod
    def to_array(value):
        return list(value.values()) if type(value) is dict else value

    @staticmethod
    def ccxt_to_crix_symbol(symbol):
        return symbol.replace('/', '_')

    @staticmethod
    def crix_to_ccxt_symbol(symbol):
        return symbol.replace('_', '/')