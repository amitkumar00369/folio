import pytest

from tests.conftest import orders_for, subscribe


def test_unknown_folio(client):
    assert client.get("/api/folios/folio-99").status_code == 404

    r = client.post("/api/subscriptions", json={"user_id": "u1", "folio_id": "folio-99", "multiplier": 1})
    assert r.status_code == 404
    assert "folio-99" in r.json()["detail"]


@pytest.mark.parametrize("multiplier", [0, -2, 2.5, "abc", None, 1000])
def test_invalid_multiplier(client, multiplier):
    r = client.post("/api/subscriptions", json={"user_id": "u1", "folio_id": "folio-1", "multiplier": multiplier})
    assert r.status_code == 400
    assert r.json()["errors"][0]["field"] == "multiplier"
    assert orders_for(client) == []


@pytest.mark.parametrize("user_id", ["", "   "])
def test_blank_user_id(client, user_id):
    r = client.post("/api/subscriptions", json={"user_id": user_id, "folio_id": "folio-1", "multiplier": 1})
    assert r.status_code == 400


def test_rebalance_unknown_folio(client):
    r = client.post(
        "/api/admin/rebalance",
        json={"folio_id": "folio-99", "outgoing_ticker": "RELIANCE", "incoming_ticker": "IDEA"},
    )
    assert r.status_code == 404


def test_rebalance_outgoing_not_in_folio(client):
    r = client.post(
        "/api/admin/rebalance",
        json={"folio_id": "folio-1", "outgoing_ticker": "WIPRO", "incoming_ticker": "IDEA"},
    )
    assert r.status_code == 400
    assert "WIPRO" in r.json()["detail"]


def test_rebalance_incoming_already_in_folio(client):
    subscribe(client, "user-101", 1)
    r = client.post(
        "/api/admin/rebalance",
        json={"folio_id": "folio-1", "outgoing_ticker": "RELIANCE", "incoming_ticker": "TCS"},
    )
    assert r.status_code == 400
    assert "already" in r.json()["detail"]

    tickers = [s["ticker"] for s in client.get("/api/folios/folio-1").json()["stocks"]]
    assert "RELIANCE" in tickers
    assert orders_for(client, source="REBALANCE") == []


@pytest.mark.parametrize("ticker", ["", "   ", "RELI ANCE", "123ABC", "TOOLONGTICKERSYMBOL123", "ID$EA"])
def test_rebalance_invalid_incoming_ticker(client, ticker):
    r = client.post(
        "/api/admin/rebalance",
        json={"folio_id": "folio-1", "outgoing_ticker": "RELIANCE", "incoming_ticker": ticker},
    )
    assert r.status_code == 400


@pytest.mark.parametrize("qty", [0, -1])
def test_rebalance_invalid_base_quantity(client, qty):
    r = client.post(
        "/api/admin/rebalance",
        json={"folio_id": "folio-1", "outgoing_ticker": "RELIANCE", "incoming_ticker": "IDEA", "incoming_base_quantity": qty},
    )
    assert r.status_code == 400


def test_tickers_are_case_insensitive(client):
    r = client.post(
        "/api/admin/rebalance",
        json={"folio_id": "folio-1", "outgoing_ticker": " reliance ", "incoming_ticker": "idea"},
    )
    assert r.status_code == 202
    assert r.json()["outgoing_ticker"] == "RELIANCE"
    assert r.json()["incoming_ticker"] == "IDEA"


def test_unknown_rebalance_job(client):
    assert client.get("/api/admin/rebalance/rb-nope").status_code == 404
