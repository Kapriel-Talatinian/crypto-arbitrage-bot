import ccxt
import pandas as pd
import time
import requests
import numpy as np
import logging
import os

TELEGRAM_TOKEN = "TOKEN" # token bot telegram
CHAT_ID = "CHATID" # id chat telegram
MAPPING_FILE = "crypto_mapping.csv"
MIN_SPREAD = 1.5  # Spread minimum en %
MIN_VOLUME = 10000  # Volume minimum en USD
HISTORY_LENGTH = 100  # Nombre de prix historiques à conserver
REFRESH_INTERVAL = 30  # Intervalle de rafraîchissement en secondes

# Configuration du logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

class CryptoArbitrageBot:
    def __init__(self):
        self.mappings = self.load_mappings()
        self.exchanges = self.initialize_exchanges()
        self.price_history = {symbol: [] for symbol in self.mappings['base_symbol']}

    def load_mappings(self):
        """Charge les mappings depuis le fichier CSV"""
        if not os.path.exists(MAPPING_FILE):
            logging.error(f"Fichier {MAPPING_FILE} introuvable !")
            exit()

        try:
            df = pd.read_csv(MAPPING_FILE)
            return {
                'base_symbol': df['base_symbol'].tolist(),
                'binance': df['binance'].tolist(),
                'coinbase': df['coinbase'].tolist(),
                'kraken': df['kraken'].tolist()
            }
        except Exception as e:
            logging.error(f"Erreur de lecture du fichier CSV: {str(e)}")
            exit()

    def initialize_exchanges(self):
        """Initialise les connexions aux exchanges"""
        exchanges = {
            'binance': ccxt.binance({'enableRateLimit': True}),
            'coinbase': ccxt.coinbaseexchange(),
            'kraken': ccxt.kraken()
        }

        for name, exchange in exchanges.items():
            try:
                exchange.load_markets()
                logging.info(f"{name.upper()} - {len(exchange.markets)} marchés chargés")
            except Exception as e:
                logging.error(f"Erreur d'initialisation {name}: {str(e)}")
                exit()

        return exchanges

    def fetch_prices(self):
        """Récupère les prix pour toutes les cryptos"""
        all_prices = {exchange: {} for exchange in self.exchanges.keys()}

        for idx, base_symbol in enumerate(self.mappings['base_symbol']):
            for exchange_name, exchange in self.exchanges.items():
                symbol = self.mappings[exchange_name][idx]
                
                try:
                    if symbol not in exchange.markets:
                        logging.warning(f"Symbole {symbol} non trouvé sur {exchange_name}")
                        continue

                    ticker = exchange.fetch_ticker(symbol)
                    if ticker['last'] and ticker['quoteVolume']:
                        all_prices[exchange_name][base_symbol] = {
                            'price': ticker['last'],
                            'volume': ticker['quoteVolume']
                        }

                        self.update_price_history(base_symbol, ticker['last'])

                except ccxt.NetworkError as e:
                    logging.warning(f"Erreur réseau {exchange_name} ({symbol}): {str(e)}")
                except ccxt.ExchangeError as e:
                    logging.warning(f"Erreur exchange {exchange_name} ({symbol}): {str(e)}")

        return all_prices

    def update_price_history(self, symbol, price):
        """Met à jour l'historique des prix"""
        if len(self.price_history[symbol]) >= HISTORY_LENGTH:
            self.price_history[symbol].pop(0)
        self.price_history[symbol].append(price)

    def calculate_volatility(self, symbol):
        """Calcule la volatilité annualisée"""
        if len(self.price_history[symbol]) < 2:
            return 0.0

        try:
            series = pd.Series(self.price_history[symbol])
            log_returns = np.log(series / series.shift(1))
            return log_returns.std() * np.sqrt(365 * 24) * 100
        except Exception as e:
            logging.error(f"Erreur calcul volatilité {symbol}: {str(e)}")
            return 0.0

    def detect_opportunities(self, prices):
        """Détecte les opportunités d'arbitrage"""
        opportunities = []

        for idx, base_symbol in enumerate(self.mappings['base_symbol']):
            exchange_prices = {}

            for exchange_name in self.exchanges.keys():
                if base_symbol in prices[exchange_name]:
                    data = prices[exchange_name][base_symbol]
                    if data['volume'] >= MIN_VOLUME:
                        exchange_prices[exchange_name] = data['price']

            if len(exchange_prices) >= 2:
                max_price = max(exchange_prices.values())
                min_price = min(exchange_prices.values())
                spread = ((max_price - min_price) / min_price) * 100

                if spread >= MIN_SPREAD:
                    opportunities.append({
                        'symbol': base_symbol,
                        'spread': spread,
                        'volatility': self.calculate_volatility(base_symbol),
                        'buy_exchange': min(exchange_prices, key=exchange_prices.get),
                        'sell_exchange': max(exchange_prices, key=exchange_prices.get),
                        'buy_price': min_price,
                        'sell_price': max_price,
                        'profit': max_price - min_price
                    })

        return opportunities

    def send_telegram_alert(self, message):
        """Envoie une alerte Telegram"""
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        try:
            response = requests.post(url, json={
                'chat_id': CHAT_ID,
                'text': message,
                'parse_mode': 'HTML'
            }, timeout=10)
            response.raise_for_status()
        except Exception as e:
            logging.error(f"Erreur d'envoi Telegram: {str(e)}")

    def format_message(self, opportunity):
        """Formate le message d'alerte"""
        return (
            f"🚀 <b>OPPORTUNITÉ D'ARBITRAGE</b> 🚀\n\n"
            f"▫️ Crypto: {opportunity['symbol']}\n"
            f"📈 Spread: {opportunity['spread']:.2f}%\n"
            f"📊 Volatilité: {opportunity['volatility']:.2f}%\n\n"
            f"🟢 Acheter sur: {opportunity['buy_exchange'].upper()}\n"
            f"💰 Prix: {opportunity['buy_price']:.2f} $\n\n"
            f"🔴 Vendre sur: {opportunity['sell_exchange'].upper()}\n"
            f"💰 Prix: {opportunity['sell_price']:.2f} $\n\n"
            f"💵 Profit potentiel: {opportunity['profit']:.2f} $"
        )

    def run(self):
        """Lance la surveillance"""
        logging.info("Démarrage du bot d'arbitrage...")
        logging.info(f"Cryptos surveillées: {', '.join(self.mappings['base_symbol'])}") 
        while True:
            try:
                start_time = time.time()
                
                # Récupération des prix
                prices = self.fetch_prices()
                
                # Détection des opportunités
                opportunities = self.detect_opportunities(prices)
                
                # Envoi des alertes
                for opp in opportunities:
                    message = self.format_message(opp)
                    self.send_telegram_alert(message)
                    logging.info(f"Alerte envoyée pour {opp['symbol']} - Spread: {opp['spread']:.2f}%")

                # Gestion du timing
                elapsed = time.time() - start_time
                sleep_time = max(REFRESH_INTERVAL - elapsed, 5)
                time.sleep(sleep_time)

            except KeyboardInterrupt:
                logging.info("Arrêt propre du bot...")
                break
            except Exception as e:
                logging.error(f"Erreur critique: {str(e)}")
                time.sleep(60)

if __name__ == "__main__":
    bot = CryptoArbitrageBot()
    bot.run()