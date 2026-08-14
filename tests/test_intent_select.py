from engineer_kit import RestConnector


def test_select_property_without_request():
    connector = RestConnector(base_url="https://example.test/orders", pagination=False, select=["id"])
    assert connector.selected_fields == ("id",)
