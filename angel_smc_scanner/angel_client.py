from dataclasses import dataclass
import logging
import threading
import time
import pyotp
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import pandas as pd

logger = logging.getLogger("nifty-smc-angel")

@dataclass
class Credentials:
    client_code: str
    api_key: str
    pin: str
    totp_secret: str

class AngelClient:
    def __init__(self, credentials):
        self.credentials = credentials
        self.api = SmartConnect(api_key=credentials.api_key)
        self.jwt = None
        self.feed_token = None
        self.ws = None
        self.ws_thread = None
        self.ws_connected = False
        self.ws_error = None
        self._ws_lock = threading.Lock()
        self._ws_stop = threading.Event()
        self._historical_lock = threading.Lock()
        self._last_historical_call = 0.0
        self._historical_block_until = 0.0
        self._extra_subscriptions = {}  # exchange type -> set of option tokens to restore after reconnect

    def login(self):
        otp = pyotp.TOTP(self.credentials.totp_secret).now()
        result = self.api.generateSession(self.credentials.client_code, self.credentials.pin, otp)
        if not result or not result.get("status"):
            raise RuntimeError((result or {}).get("message", "Angel One login failed"))
        data = result.get("data") or {}
        self.jwt = data.get("jwtToken")
        self.feed_token = self.api.getfeedToken()
        if not self.jwt or not self.feed_token:
            raise RuntimeError("Login response did not contain JWT/feed token.")
        return True

    def historical(self, exchange, symbol_token, interval, start, end):
        if not self.jwt:
            raise RuntimeError("Login required.")
        params = {
            "exchange": exchange, "symboltoken": str(symbol_token), "interval": interval,
            "fromdate": start.strftime("%Y-%m-%d %H:%M"),
            "todate": end.strftime("%Y-%m-%d %H:%M"),
        }
        # Serialize historical requests and space them to avoid burst calls.
        with self._historical_lock:
            now = time.monotonic()
            if now < self._historical_block_until:
                wait = int(self._historical_block_until - now)
                raise RuntimeError(f"Angel One historical API cooldown active ({wait}s remaining) after rate limit. Reuse local cache; do not retry repeatedly.")
            spacing = 3.0 - (now - self._last_historical_call)
            if spacing > 0:
                time.sleep(spacing)
            self._last_historical_call = time.monotonic()
            try:
                response = self.api.getCandleData(params)
            except Exception as exc:
                msg = str(exc)
                if "rate" in msg.lower() or "access denied" in msg.lower():
                    self._historical_block_until = time.monotonic() + 300
                raise RuntimeError(f"Angel One historical request failed: {msg[:220]}") from exc
        if not response or not response.get("status"):
            message = (response or {}).get("message", "Historical data request failed")
            if "rate" in str(message).lower() or "access denied" in str(message).lower():
                self._historical_block_until = time.monotonic() + 300
            raise RuntimeError(str(message))
        rows = response.get("data") or []
        frame = pd.DataFrame(rows, columns=["datetime", "open", "high", "low", "close", "volume"])
        if frame.empty:
            return frame
        frame["datetime"] = pd.to_datetime(frame["datetime"])
        for col in ["open", "high", "low", "close", "volume"]:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
        return frame.dropna().sort_values("datetime").drop_duplicates("datetime").reset_index(drop=True)

    def ltp(self, exchange, tradingsymbol, symbol_token):
        if not self.jwt:
            raise RuntimeError("Login required.")
        response = self.api.ltpData(exchange, tradingsymbol, str(symbol_token))
        if not response or not response.get("status"):
            raise RuntimeError((response or {}).get("message", "LTP request failed"))
        data = response.get("data") or {}
        price = data.get("ltp")
        if price is None:
            raise RuntimeError("Angel One LTP response did not contain a price.")
        return {"price": float(price), "exchange": exchange, "tradingsymbol": tradingsymbol,
                "symbol_token": str(symbol_token),
                "raw_time": data.get("exchangeTimestamp") or data.get("exchange_timestamp")}

    def start_market_websocket(self, symbol_token, on_tick, exchange_type=1):
        """Start/restart SmartAPI WebSocket 2.0 with bounded backoff; market-data only."""
        if not self.jwt or not self.feed_token:
            raise RuntimeError("Login required before starting the market WebSocket.")
        with self._ws_lock:
            if self.ws_thread and self.ws_thread.is_alive():
                return {"started": True, "already_running": True}
            self._ws_stop.clear()

            def run_socket():
                delay = 2
                while not self._ws_stop.is_set():
                    ws = SmartWebSocketV2(
                        self.jwt, self.credentials.api_key, self.credentials.client_code, self.feed_token
                    )
                    self.ws = ws
                    def on_open(wsapp):
                        self.ws_connected = True
                        self.ws_error = None
                        logger.info("Angel One WebSocket connected; subscribing to NIFTY token %s", symbol_token)
                        base_tokens = [str(symbol_token)]
                        ws.subscribe(correlation_id="nifty-smc-live", mode=1,
                                     token_list=[{"exchangeType": int(exchange_type), "tokens": base_tokens}])
                        for extra_exchange, extra_tokens in list(self._extra_subscriptions.items()):
                            if extra_tokens:
                                ws.subscribe(correlation_id="nifty-option", mode=1,
                                             token_list=[{"exchangeType": int(extra_exchange), "tokens": sorted(extra_tokens)}])
                    def on_data(wsapp, message):
                        try:
                            if isinstance(message, dict): on_tick(message)
                        except Exception:
                            logger.exception("Could not process WebSocket tick")
                    def on_error(wsapp, error):
                        self.ws_error = str(error)[:300]
                        self.ws_connected = False
                        logger.error("Angel One WebSocket error: %s", self.ws_error)
                    def on_close(wsapp):
                        self.ws_connected = False
                        logger.warning("Angel One WebSocket closed; reconnect loop will retry")
                    ws.on_open = on_open
                    ws.on_data = on_data
                    ws.on_error = on_error
                    ws.on_close = on_close
                    try:
                        ws.connect()
                    except Exception as exc:
                        self.ws_error = str(exc)[:300]
                        self.ws_connected = False
                        logger.exception("WebSocket connection attempt failed")
                    finally:
                        self.ws_connected = False
                    if self._ws_stop.is_set(): break
                    self._ws_stop.wait(delay)
                    delay = min(delay * 2, 30)

            self.ws_thread = threading.Thread(target=run_socket, name="angel-smartapi-ws", daemon=True)
            self.ws_thread.start()
            return {"started": True, "already_running": False}

    def subscribe_market_tokens(self, tokens, exchange_type=2, mode=1):
        """Remember and subscribe additional market-data tokens, including across reconnects."""
        clean = [str(x) for x in tokens if str(x).strip()]
        if not clean:
            return False
        bucket = self._extra_subscriptions.setdefault(int(exchange_type), set())
        bucket.update(clean)
        if not self.ws_connected or not self.ws:
            return False  # queued; on_open will subscribe them after the next connection
        self.ws.subscribe(correlation_id="nifty-option", mode=int(mode),
                          token_list=[{"exchangeType": int(exchange_type), "tokens": clean}])
        return True

    def unsubscribe_market_tokens(self, tokens, exchange_type=2, mode=1):
        """Unsubscribe market-data tokens and remove them from reconnect subscriptions."""
        clean = [str(x) for x in tokens if str(x).strip()]
        if not clean:
            return False
        bucket = self._extra_subscriptions.setdefault(int(exchange_type), set())
        bucket.difference_update(clean)
        if not self.ws_connected or not self.ws:
            return False
        self.ws.unsubscribe(correlation_id="nifty-option", mode=int(mode),
                            token_list=[{"exchangeType": int(exchange_type), "tokens": clean}])
        return True

    def close_market_websocket(self):
        self._ws_stop.set()
        with self._ws_lock:
            if self.ws:
                try:
                    self.ws.close_connection()
                except Exception:
                    logger.exception("Error closing SmartAPI WebSocket")
            self.ws_connected = False
