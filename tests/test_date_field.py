from datetime import date

from engineer_kit.connectors.date_field import extract_date_value


def test_extracts_nested_dotted_path():
    record = {"commit": {"author": {"date": "2024-03-15T10:00:00Z"}}}
    assert extract_date_value(record, "commit.author.date") == date(2024, 3, 15)


def test_returns_none_when_path_does_not_exist():
    record = {"commit": {"author": {}}}
    assert extract_date_value(record, "commit.author.date") is None


def test_returns_none_when_intermediate_path_is_not_a_dict():
    record = {"commit": "nao-e-um-dict"}
    assert extract_date_value(record, "commit.author.date") is None


def test_accepts_plain_date_string_without_time():
    record = {"updated_at": "2024-03-15"}
    assert extract_date_value(record, "updated_at") == date(2024, 3, 15)


def test_accepts_callable_spec():
    record = {"foo": "2024-03-15T10:00:00Z"}
    assert extract_date_value(record, lambda r: r["foo"]) == date(2024, 3, 15)


def test_invalid_date_string_returns_none():
    record = {"updated_at": "nao e uma data"}
    assert extract_date_value(record, "updated_at") is None
