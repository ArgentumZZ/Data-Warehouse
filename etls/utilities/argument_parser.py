import datetime, sys, re
from typing import Optional, Tuple
import utilities.logging_manager as lg

def parse_arguments(settings) -> Tuple[Optional[str], str, int]:
    """
    Parse and validate command-line arguments passed from the .bat launcher.

    This function overrides default ETL settings based on user-supplied arguments.
    It supports:
        - 0 arguments → use defaults
        - 1 argument  → invalid (date must be paired with config)
        - 2+ arguments → validate the first two, ignore the rest for now

    Argument rules:
        1. First argument must be a valid date in YYYY-MM-DD format.
        2. Second argument must be in the form config=FX, where X is an integer.
           X becomes the new max_days_to_load.
        3. Arguments 3, 4, 5 are logged but not yet validated.

    Example:
        If the user runs:
            python argument_parser.py 2025-01-01 config=F30 TEST OMG HELLO

        Then:
            sys.argv[0] = "argument_parser.py"
            sys.argv[1] = "2025-01-01"
            sys.argv[2] = "config=F30"
            sys.argv[3] = "TEST"
            sys.argv[4] = "OMG"
            sys.argv[5] = "HELLO"

    Parameters
    ----------
    settings : object
        A settings object containing default values:
            - settings.load_type
            - settings.max_days_to_load

    Returns
    -------
    tuple
        (forced_sdt, load_type, max_days_to_load)
            forced_sdt:                → overridden start date or None
            load_type:                 → "F" or default from settings
            max_days_to_load           → overridden or default value
    """
    lg.info(f"DEBUG sys.argv: {sys.argv}")
    forced_sdt = None
    load_type = None
    max_days_to_load = None


    lg.info(f"The script name: {sys.argv[0]}")

    # The number of user-supplied arguments:
    num_arguments = len(sys.argv) - 1

    # Case 1. No arguments
    if num_arguments == 0:
        # No arguments provided → acceptable, set the defaults
        print("No arguments provided. Running with defaults.")

    # Case 2. One argument
    if num_arguments == 1:
        # not allowed
        raise ValueError("You must provide either 0 arguments or at least 2 arguments. If you provide arguments, the first must be YYYY-MM-DD and the second must be config=FX.")

    # 3. Two or more arguments
    if num_arguments >= 2:
        arg1 = sys.argv[1]
        arg2 = sys.argv[2]
        arg3 = sys.argv[3]
        arg4 = sys.argv[4]
        arg5 = sys.argv[5]

        lg.info(f"Argument 1: {arg1}")
        lg.info(f"Argument 2: {arg2}")
        lg.info(f"Argument 3: {arg3}")
        lg.info(f"Argument 4: {arg4}")
        lg.info(f"Argument 5: {arg5}")

        try:
            datetime.datetime.strptime(arg1, "%Y-%m-%d")

            # override the start date time with the first argument
            forced_sdt = arg1
        except ValueError:
            raise ValueError("First argument must be a valid date in YYYY-MM-DD format.")

        # Validate the second argument: config=F<number>
        if not re.match(r"^config=F\d+$", arg2):
            raise ValueError("In addition to a first date argument (YYYY-MM-DD), a second argument is needed in the format config=FX where X is an integer.")
        else:
            load_type = "F"
            # if arg2 = "config=F30"
            # then arg2.split("=", 1) -> ["config", "F30"]
            # then arg.split("=", 1)[1] -> ["F30"]
            # then arg.split("=", 1)[1][1:] -> "30"
            # Defensively, use .strip() to remove any whitespace
            x = arg2.split("=", 1)[1][1:]
            x = x.strip()

            # If no number is provided, fallback to default settings
            if x == "":
                max_days_to_load = settings.max_days_to_load
            else:
                max_days_to_load = int(x)

        # Future validation logic for arguments 3, 4 and 5

    # Defaults if not overridden
    # Use the one from the script_parameters.py file
    if load_type is None:
        load_type = settings.load_type

    # Use the one from the script_parameters.py file
    if max_days_to_load is None:
        max_days_to_load = settings.max_days_to_load

    return forced_sdt, load_type, max_days_to_load