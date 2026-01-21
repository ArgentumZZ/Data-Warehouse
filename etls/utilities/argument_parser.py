import sys, re
from typing import Optional, Tuple, Any
import utilities.logging_manager as lg

def parse_arguments(settings: Any) -> Tuple[Optional[str], str, int]:
    """
    1. A flexible argument parser supporting any order and optional parameters.
    2. Enforces <name>=<value> format.
    3. sdt and config arguments must be passed together, otherwise a ValueError will be raised.
    4. sdt overrides the default start date time, in config=F<number> <number> is an integer
    that overrides the default max_days_to_load.
    5. edt and project have been added but not logic has been implemented for them.

    Args:
        settings: the file for the project script parameters

    Returns:
        A tuple of start date time, load type and maximum number of days to load
    """

    lg.info(f"DEBUG sys.argv: {sys.argv}")

    # 1. Define a dictionary storage for parsed arguments
    parsed = {}

    # 2. Define regex validation rules for each argument
    validation_rules = {
        "sdt"       : r"^\d{4}-\d{2}-\d{2}$",
        "edt"       : r"^\d{4}-\d{2}-\d{2}$",
        "config"    : r"^F\d*$",
        "project"   : r"^[A-Za-z0-9_]+$",
    }

    # 3. Iterate over all arguments (except sys.argv[0] = ... .run_script_py name)
    for argument in sys.argv[1:]:

        # 4. Check if equality exists in the argument
        if "=" not in argument:
            raise ValueError(f"Invalid argument '{argument}'. Expected format <name>=<value>.")

        # 5. Split the argument by (maximum one) equality sign
        name, value = argument.split("=", 1)

        # 6. Check if the name exists in the validation_rules dictionary
        if name not in validation_rules.keys():
            raise ValueError(f"Unknown argument '{name}'. Allowed arguments: {list(validation_rules.keys())}")

        # 7. Check if duplicated argument names were provided
        if name in parsed:
            raise ValueError(f"Duplicate argument '{name}' is not allowed.")

        # 8. Assign the regex pattern to a variable
        pattern = validation_rules[name]

        # 9. Check if the value matches the regex pattern
        if not re.match(pattern, value):
            raise ValueError(f"Invalid value for '{name}': '{value}'.")

        # 10. Log the argument
        parsed[name] = value
        lg.info(f"Parsed argument: {name}={value}")

    # 11. Require both, start date time (=sdt) and config arguments, to be provided.
    if ("sdt" in parsed and "config" not in parsed) or ("sdt" not in parsed and "config" in parsed):
        raise ValueError("Arguments 'sdt' and 'config' must be provided together.")

    # 12. Extract default values
    forced_sdt = None
    load_type = settings.load_type
    max_days_to_load = settings.max_days_to_load

    # 13. Override forced_sdt with start date time value (=sdt)
    if "sdt" in parsed:
        forced_sdt = parsed["sdt"]

    # 14. Override the max_days_to_load with <number> from config=F<number>
    # If config=F was provided, use the default max_days_to_load
    if "config" in parsed:
        load_type = "F"
        value = parsed["config"][1:]  # strip the F and select the value

        # If config=F was provided (no override), use the default max_days_to_load
        if value == "":
            max_days_to_load = settings.max_days_to_load
        else:
            max_days_to_load = int(value)

    return forced_sdt, load_type, max_days_to_load