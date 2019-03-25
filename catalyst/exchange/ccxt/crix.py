import hmac
import json
from datetime import datetime
import ccxt

import requests

class APIError(RuntimeError):
    """
    General exception for API calls
    """

    operation: str  #: operation name
    code: int  #: HTTP response code
    text: str  #: error description

    def __init__(self, operation: str, code: int, text: str) -> None:
        self.code = code
        self.operation = operation
        self.text = text
        super().__init__(
            'API ({}) error: code {}: {}'.format(operation, code, text)
        )

    @staticmethod
    def ensure(operation: str, req: requests.Response):
        """
        Ensure status code of HTTP request and raise exception if needed
        :param operation: logical operation name
        :param req: request's response object
        """
        if req.status_code not in (200, 204):
            raise APIError(operation, req.status_code, req.text)

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
        self.name = 'Crix'
        self.markets = {}
        self.timeframes = {
            '1m': '1',
            '5m': '5',
            '15m': '15',
            '30m': '30',
            '1h': '60',
            '2h': '120',
            '4h': '240',
            '1d': 'D'
        }

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
        req = requests.get(self._base_url + '/info/symbols')
        APIError.ensure('load_markets', req)
        data = req.json()
        if (
                'symbol' in data
            and isinstance(data['symbol'], list)
            and data['symbol']
        ):
            for item in data['symbol']:
                id = item['symbolName']
                base = item['base']
                quote = item['quote']
                symbol = base + '/' + quote
                active = item['trading']
                precision = {
                    'base': int(item['basePrecision']),
                    'quote': int(item['quotePrecision'])
                }
                limits = {
                    'amount': {
                        'min': float(item['minLot']),
                        'max': float(item['maxLot'])
                    },
                    'price': {
                        'min': float(item['minPrice']),
                        'max': float(item['maxPrice'])
                    }
                }
                maker = item['makerFee']
                taker = item['takerFee']

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

    def fetch_ohlcv(self, symbol, timeframe='1m', since=None, limit=10, params={}):
        """
        Get K-Lines for specific symbol in a time frame
        :param symbol: K-Line symbol name
        :param timeframe: K-line resolution (by default 1-minute)
        :param since: earliest interesting time
        :param limit: maximum number of entries in a response;
        Valid limits:[1, 5, 10, 20, 50, 100, 500, 1000]
        :return: list of ticker
        """
        ret = []
        valid_limits = [1, 5, 10, 20, 50, 100, 500, 1000]
        if not self.markets:
            self.load_markets()
        
        req_symbol = self.ccxt_to_crix_symbol(symbol)
        req_timeframe = self.timeframes[timeframe]
        if limit not in valid_limits:
            print(
                "Exch[%s] fetch_ohlcv() got unsupported limit[%s].\
                Valid limits are: %s"
                % (self.name, limit, valid_limits)
            )
            return ret
        
        req = requests.post(self._base_url + '/klines', json={
            'req': {
                'startTime': since,
                'endTime': int(datetime.now().timestamp() * 1000),
                'symbolName': req_symbol,
                'resolution': req_timeframe,
                'limit': limit,
            }
        })
        APIError.ensure('fetch-ohlcv', req)
        data = req.json()
        if (
                'ohlc' in data
            and isinstance(data['ohlc'], list)
            and data['ohlc']
        ):
            for item in data['ohlc']:
                ts = item['openTime']
                open_price = float(item['open'])
                high = float(item['high'])
                low = float(item['low'])
                close = float(item['close'])
                vol = float(item['volume'])

                ret.append(
                    [ts, open_price, high, low, close, vol]
                )
        return ret

    @staticmethod
    def to_array(value):
        return list(value.values()) if type(value) is dict else value

    @staticmethod
    def ccxt_to_crix_symbol(symbol):
        ret = symbol
        if '/' in symbol:
            ret = symbol.replace('/', '_')
        return ret