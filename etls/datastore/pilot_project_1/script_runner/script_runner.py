import sys
import os

# The .bat file now handles the PYTHONPATH, so we can import directly
from script_factory.script_factory import ScriptFactory
import utilities.logging_manager as lg

def main():
    """
    Main entry point for the ETL run.

    - Initializes the ScriptFactory
    - Executes tasks sequentially
    - Logs progress and errors
    - Exits with code 0 if successful, 1 if any task fails
    """
    lg.info("Starting ETL run")  # Log the start of the ETL
    success = True  # Flag to track overall ETL success

    try:
        # 1. Initialize the ETL factory and retrieve tasks
        factory = ScriptFactory()
        tasks = factory.get_tasks()

        # 2. Run each task sequentially
        for task in tasks:
            lg.info(f"Running task: {task.__name__}")
            try:
                task()  # Execute the task
            except Exception as e:
                # 2a. Log task failure and traceback
                lg.error(f"Task {task.__name__} failed: {e}")
                lg.exception("Full traceback:")
                success = False
                break  # Stop execution if a critical task fails

        # 3. Determine exit based on success flag
        if success:
            lg.info("ETL run completed successfully. \nReturn code: 0")
            sys.exit(0)  # Exit code 0 indicates success
        else:
            lg.error("ETL run finished with errors. \nReturn code: 1")
            sys.exit(1)  # Exit code 1 indicates failure

    except Exception as e:
        # 4. Handle unexpected initialization or system-level errors
        lg.error(f"Critical error during initialization: {e}")
        lg.exception("Full traceback:")
        sys.exit(1)  # Exit code 1 indicates failure

# 5. Run main() if this script is executed directly
if __name__ == "__main__":
    main()
