import re


def from_string_to_number(value: str) -> int:
    if re.match("^\\d+$", value):
        return int(value)
    decimal_value = float(value[0:-1])
    suffix = value[-1].upper()
    match suffix:
        case "P":
            decimal_value *= 1e15
        case "T":
            decimal_value *= 1e12
        case "G":
            decimal_value *= 1e9
        case "M":
            decimal_value *= 1e6
        case "K":
            decimal_value *= 1e3
        case _:
            raise ValueError("Unknown suffix")

    return round(decimal_value)


def from_number_to_string(value: float) -> str:
    def try_round(value: float | int):
        if round(value) == value:
            return int(value)
        return value

    if value < 1e3:
        return f"{value}"
    elif value < 1e6:
        return f"{try_round(value / 1e3)} K"
    elif value < 1e9:
        return f"{try_round(value / 1e6)} M"
    elif value < 1e12:
        return f"{try_round(value / 1e9)} G"
    elif value < 1e15:
        return f"{try_round(value / 1e12)} T"
    elif value < 1e18:
        return f"{try_round(value / 1e15)} P"
    else:
        return f"{try_round(value / 1e18)} E"
