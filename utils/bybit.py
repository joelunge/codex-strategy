import time
import hmac
import hashlib
from urllib.parse import urlencode
import requests

TEST_BASE_URL = "https://api-testnet.bybit.com"
MAIN_BASE_URL = "https://api.bybit.com"


def base_url(net: str) -> str:
    if net == "testnet":
        return TEST_BASE_URL
    if net == "mainnet":
        return MAIN_BASE_URL
    raise ValueError("net must be 'testnet' or 'mainnet'")


def get_mark_price(symbol: str, net: str = "testnet") -> float:
    url = base_url(net) + "/v5/market/tickers"
    params = {"category": "linear", "symbol": symbol}
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    return float(data["result"]["list"][0]["markPrice"])


def _auth_params(key: str, secret: str, params: dict) -> dict:
    ts = str(int(time.time() * 1000))
    recv = "5000"
    qs = urlencode(sorted(params.items()))
    signature = hmac.new(
        secret.encode(), f"{ts}{key}{recv}{qs}".encode(), hashlib.sha256
    ).hexdigest()
    auth = {"apiKey": key, "timestamp": ts, "recvWindow": recv, "sign": signature}
    return {**params, **auth}


def place_order_post_only(
    symbol: str,
    side: str,
    qty: float,
    price: float,
    key: str,
    secret: str,
    net: str = "testnet",
):
    url = base_url(net) + "/v5/order/create"
    params = {
        "category": "linear",
        "symbol": symbol,
        "side": side,
        "orderType": "Limit",
        "qty": qty,
        "price": price,
        "timeInForce": "PostOnly",
    }
    payload = _auth_params(key, secret, params)
    resp = requests.post(url, json=payload, timeout=10)
    resp.raise_for_status()
    return resp.json()


def fetch_funding(symbol: str, net: str = "testnet") -> float:
    url = base_url(net) + "/v5/market/funding/prev-funding-rate"
    params = {"symbol": symbol}
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    return float(data["result"]["list"][0]["fundingRate"])
