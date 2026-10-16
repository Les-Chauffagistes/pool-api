import pytest

from pool_api.utils import format_bitcoin_subversion, from_number_to_string, from_string_to_number


@pytest.mark.parametrize(
    "value, expected",
    [
        (0, "0"),
        (12.5, "12.5"),
        (999, "999"),
        (1_000, "1 K"),
        (1_500, "1.5 K"),
        (999_999, "999.999 K"),
        (1_000_000, "1 M"),
        (2_500_000, "2.5 M"),
        (1e9, "1 G"),
        (1e12, "1 T"),
        (3.2e15, "3.2 P"),
        (1e18, "1 E"),
        (5e19, "50 E"),
    ],
)
def test_from_number_to_string(value, expected):
    assert from_number_to_string(value) == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        ("0", 0),
        ("1234", 1234),
        ("1K", 1_000),
        ("1.5k", 1_500),
        ("2M", 2_000_000),
        ("1.5G", 1_500_000_000),
        ("3T", 3 * 10**12),
        ("1.25P", 1_250_000_000_000_000),
    ],
)
def test_from_string_to_number(value, expected):
    assert from_string_to_number(value) == expected


@pytest.mark.parametrize("value", ["12X", "abc", "", "K"])
def test_from_string_to_number_rejects_invalid(value):
    with pytest.raises(ValueError):
        from_string_to_number(value)


@pytest.mark.parametrize("value", [1_000, 1_500_000, 2_000_000_000, 4 * 10**12])
def test_round_trip(value):
    assert from_string_to_number(from_number_to_string(value).replace(" ", "")) == value


@pytest.mark.parametrize(
    "version, expected",
    [(280100, "/Satoshi:28.1.0/"), (270000, "/Satoshi:27.0.0/"), (300203, "/Satoshi:30.2.3/")],
)
def test_format_bitcoin_subversion(version, expected):
    assert format_bitcoin_subversion(version) == expected
