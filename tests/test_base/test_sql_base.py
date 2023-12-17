from __future__ import annotations

from qaspen.base.text import Text


def test_text_querystring_method() -> None:
    """Test `querystring` method."""
    text = Text("SELECT * FROM qaspen")

    querystring, qs_params = text.querystring().build()
    assert querystring == "SELECT * FROM qaspen"
    assert not qs_params
