from tests.conftest import base_quantities, orders_for, subscribe


def test_seven_folios_seeded_with_twelve_stocks(client):
    folios = client.get("/api/folios").json()
    assert len(folios) == 7
    for folio in folios:
        assert len(folio["stocks"]) == 12
        assert all(s["base_quantity"] > 0 for s in folio["stocks"])
        assert len({s["ticker"] for s in folio["stocks"]}) == 12


def test_subscribe_creates_twelve_buys_with_multiplier(client):
    base = base_quantities(client)
    body = subscribe(client, "user-101", 3)

    sub = body["subscription"]
    assert sub["active"] is True
    assert sub["multiplier"] == 3
    assert sub["folio_name"] == "Alpha Growth"

    orders = body["orders"]
    assert len(orders) == 12
    assert {o["action"] for o in orders} == {"BUY"}
    assert {o["status"] for o in orders} == {"EXECUTED"}
    for o in orders:
        assert o["quantity"] == base[o["ticker"]] * 3
        assert o["user_id"] == "user-101"
        assert o["order_id"].startswith("ord-")

    positions = {p["ticker"]: p["quantity"] for p in sub["positions"]}
    assert positions == {t: q * 3 for t, q in base.items()}


def test_subscription_is_persisted(client):
    sub_id = subscribe(client, "user-101", 2)["subscription"]["id"]

    subs = client.get("/api/users/user-101/subscriptions").json()
    assert [s["id"] for s in subs] == [sub_id]
    assert subs[0]["multiplier"] == 2

    assert len(orders_for(client, subscription_id=sub_id)) == 12


def test_different_users_keep_their_own_multiplier(client):
    base = base_quantities(client)
    subscribe(client, "user-101", 3)
    subscribe(client, "user-202", 5)

    for user, mult in [("user-101", 3), ("user-202", 5)]:
        orders = orders_for(client, user_id=user)
        assert len(orders) == 12
        assert all(o["quantity"] == base[o["ticker"]] * mult for o in orders)


def test_cannot_hold_two_active_subscriptions_to_same_folio(client):
    subscribe(client, "user-101", 1)
    r = client.post(
        "/api/subscriptions", json={"user_id": "user-101", "folio_id": "folio-1", "multiplier": 2}
    )
    assert r.status_code == 409
    assert len(orders_for(client, user_id="user-101")) == 12


def test_can_resubscribe_after_exit(client):
    sub_id = subscribe(client, "user-101", 1)["subscription"]["id"]
    client.post(f"/api/subscriptions/{sub_id}/exit")
    subscribe(client, "user-101", 4)

    active = client.get("/api/users/user-101/subscriptions", params={"active": True}).json()
    assert len(active) == 1 and active[0]["multiplier"] == 4
def test_list_all_subscriptions(client):
    a = subscribe(client, "user-101", 3)["subscription"]["id"]
    subscribe(client, "user-202", 5)
    subscribe(client, "user-303", 2, folio_id="folio-2")
    client.post(f"/api/subscriptions/{a}/exit")

    assert len(client.get("/api/subscriptions").json()) == 3
    assert len(client.get("/api/subscriptions", params={"folio_id": "folio-1"}).json()) == 2
    active = client.get("/api/subscriptions", params={"folio_id": "folio-1", "active": True}).json()
    assert [s["user_id"] for s in active] == ["user-202"]
    assert len(client.get("/api/subscriptions", params={"limit": 1}).json()) == 1