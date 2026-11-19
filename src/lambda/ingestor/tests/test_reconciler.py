from reconciler import business_hash
from watchlist import is_terminal


def test_hash_ignores_ingestion_metadata_and_key_order():
    a = {"order_code": "A", "status": "delivering", "weight": 100}
    b = {"weight": 100, "status": "delivering", "order_code": "A", "ingested_at": "later"}
    assert business_hash(a) == business_hash(b)
    b["weight"] = 200
    assert business_hash(a) != business_hash(b)


def test_return_process_and_delivery_failure_remain_open():
    for status in (
        "delivery_fail",
        "return_fail",
        "waiting_to_return",
        "return",
        "returning",
        "return_transporting",
        "return_sorting",
    ):
        assert not is_terminal(status)
    for status in ("delivered", "returned", "cancel", "exception", "lost", "damage", "scrap"):
        assert is_terminal(status)
