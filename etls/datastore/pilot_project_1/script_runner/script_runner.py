import sys
import os

# The .bat file now handles the PYTHONPATH, so we can import directly
from script_factory.script_factory import ScriptFactory
import utilities.logging_manager as lg

def main():
    lg.info("Starting ETL run")
    success = True  # Track if everything worked

    try:
        factory = ScriptFactory()
        tasks = factory.get_tasks()

        for task in tasks:
            lg.info(f"Running task: {task.__name__}")
            try:
                task()
            except Exception as e:
                lg.error(f"Task {task.__name__} failed: {e}")
                success = False
                break  # Stop if a critical task (like connection) fails

        if success:
            lg.info("ETL run completed successfully")
        else:
            lg.error("ETL run finished with errors.")

    except Exception as e:
        lg.error(f"Critical error during initialization: {e}")

if __name__ == "__main__":
    main()