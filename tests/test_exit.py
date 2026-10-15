from tests.conftest import base_quantities, orders_for, subscribe


def test_exit_sells_all_twelve_positions(client):
    base = base_quantities(client)
    sub_id = subscribe(client, "user-101", 3)["subscription"]["id"]

    r = client.post(f"/api/subscriptions/{sub_id}/exit")
    assert r.status_code == 200
    body = r.json()

    assert body["subscription"]["active"] is False
    assert body["subscription"]["exited_at"] is not None
    assert body["subscription"]["positions"] == []

    sells = body["orders"]
    assert len(sells) == 12
    assert {o["action"] for o in sells} == {"SELL"}
    assert {o["ticker"]: o["quantity"] for o in sells} == {t: q * 3 for t, q in base.items()}


def test_exit_only_touches_that_subscription(client):
    a = subscribe(client, "user-101", 3)["subscription"]["id"]
    b = subscribe(client, "user-202", 5)["subscription"]["id"]

    client.post(f"/api/subscriptions/{a}/exit")

    assert client.get(f"/api/subscriptions/{b}").json()["active"] is True
    assert orders_for(client, user_id="user-202", source="EXIT") == []


def test_exit_twice_is_rejected(client):
    sub_id = subscribe(client, "user-101", 1)["subscription"]["id"]
    assert client.post(f"/api/subscriptions/{sub_id}/exit").status_code == 200

    r = client.post(f"/api/subscriptions/{sub_id}/exit")
    assert r.status_code == 409
    assert "already exited" in r.json()["detail"]
    assert len(orders_for(client, subscription_id=sub_id, source="EXIT")) == 12


def test_exit_unknown_subscription(client):
    r = client.post("/api/subscriptions/sub-nope/exit")
    assert r.status_code == 404
