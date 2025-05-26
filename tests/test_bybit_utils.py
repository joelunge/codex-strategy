from utils.bybit import base_url, get_index_price


class DummyResponse:
    def raise_for_status(self):
        pass

    def json(self):
        return {"result": {"list": [{"indexPrice": "123.45"}]}}


def test_get_index_price(monkeypatch):
    captured = {}

    def fake_get(url, params=None, timeout=10):
        captured["url"] = url
        captured["params"] = params
        return DummyResponse()

    monkeypatch.setattr("utils.bybit.requests.get", fake_get)
    price = get_index_price("BTCUSDT")
    assert price == 123.45
    assert captured["url"].endswith("/v5/market/tickers")


def test_base_url():
    assert base_url('testnet') == 'https://api-testnet.bybit.com'
    assert base_url('mainnet') == 'https://api.bybit.com'
