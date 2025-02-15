import ccxt
exchange = ccxt.kraken()
exchange = ccxt.coinbase()
exchange = ccxt.binance()

exchange.load_markets()

print(exchange.symbols) 