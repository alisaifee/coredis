from coredis.utils import AnyDict


class TestAnyDict:
    def test_empty_dict(self):
        assert AnyDict() == {}

    def test_regular_access(self):
        data = AnyDict({"a": 1, b"a": 2, "c": [1, 2, 3], "d": {1, 2, 3}, "e": {"a": 1}})
        assert data["a"] == 1
        assert data[b"a"] == 2
        assert data["c"] == [1, 2, 3]
        assert data["d"] == {1, 2, 3}
        assert data["e"] == {"a": 1}

        assert "a" in data
        assert b"a" in data

    def test_access_str_for_bytes_key(self):
        data = AnyDict({b"a": 1, "b": 2})
        assert "a" in data
        assert data["a"] == 1
        data["a"] = 2
        assert data[b"a"] == data["a"] == 2
        data[b"a"] = 3
        assert data[b"a"] == data["a"] == 3
        assert len(data) == 2
