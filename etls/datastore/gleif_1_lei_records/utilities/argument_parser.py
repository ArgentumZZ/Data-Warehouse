import datetime

def parse_arguments(argv, settings):
    forced_sdt = None
    load_type = None
    max_days_to_load = None

    for argument in argv[1:]:

        # 1. Date argument (YYYY-MM-DD)
        try:
            forced_sdt = datetime.datetime.strptime(argument, "%Y-%m-%d").date()
            continue
        except ValueError:
            pass

        # 2. config=F*
        if argument.lower().startswith("config=f"):
            load_type = "F"

            # Extract everything after the F
            x = argument.split("=", 1)[1][1:]
            x = x.strip()

            # If no number provided, fallback to settings
            if x == "":
                max_days_to_load = settings.max_days_to_load
            else:
                max_days_to_load = int(x)

    # Defaults if not overridden
    if load_type is None:
        load_type = settings.load_type

    if max_days_to_load is None:
        max_days_to_load = settings.max_days_to_load

    # Convert forced_sdt to UTC datetime
    if forced_sdt:
        forced_sdt = datetime.datetime.combine(
            forced_sdt,
            datetime.time.min
        ).replace(tzinfo=datetime.timezone.utc)

    return forced_sdt, load_type, max_days_to_load
