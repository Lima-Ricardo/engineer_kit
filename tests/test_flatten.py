from engineer_kit.storage.flatten import flatten_record


def test_flatten_simple_dict_is_unchanged():
    assert flatten_record({"a": 1, "b": "x"}) == {"a": 1, "b": "x"}


def test_flatten_joins_nested_keys_with_underscore():
    record = {"commit": {"author": {"name": "Alice", "date": "2024-01-01"}}}
    assert flatten_record(record) == {
        "commit_author_name": "Alice",
        "commit_author_date": "2024-01-01",
    }


def test_flatten_resolves_collision_by_full_path_not_order():
    record = {
        "commit": {
            "author": {"date": "2024-01-01"},
            "committer": {"date": "2024-01-02"},
        }
    }
    flat = flatten_record(record)
    assert flat["commit_author_date"] == "2024-01-01"
    assert flat["commit_committer_date"] == "2024-01-02"
    assert len(flat) == 2  # nenhuma colisao, cada caminho e uma coluna distinta


def test_flatten_lists_become_json_strings():
    record = {"parents": [{"sha": "abc"}, {"sha": "def"}]}
    flat = flatten_record(record)
    assert flat["parents"] == '[{"sha": "abc"}, {"sha": "def"}]'


def test_flatten_none_stays_none():
    record = {"a": None}
    assert flatten_record(record) == {"a": None}


def test_flatten_normalizes_unsafe_key_characters():
    record = {"user-agent": {"raw value": "x"}}
    flat = flatten_record(record)
    assert flat == {"user_agent_raw_value": "x"}
