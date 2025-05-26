from utils.bybit import base_url


def test_base_url():
    assert base_url('testnet') == 'https://api-testnet.bybit.com'
    assert base_url('mainnet') == 'https://api.bybit.com'
