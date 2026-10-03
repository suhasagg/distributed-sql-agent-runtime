from app.services.runtime_service import canonical_hash
def test_hash_is_canonical():
    assert canonical_hash({"a":1,"b":2}) == canonical_hash({"b":2,"a":1})
