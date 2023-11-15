import pytest

from qaspen.exceptions import DatabaseUrlParseError
from qaspen.utils.engine_utils import parse_database

pytesmark = [pytest.mark.anyio]


def test_correct_parse_database() -> None:
    """Test `correct` url parsing."""
    assert (
        parse_database("postgresql://username:password@hostname:port/database")
        == "database"
    )


def test_raise_parse_database() -> None:
    """Test `incorrect` url parsing, so exception in raised."""
    with pytest.raises(DatabaseUrlParseError):
        parse_database("postgresql://username:password@hostname:port")
