import sys
import os

# The .bat file now handles the PYTHONPATH, so we can import directly
from script_factory.script_factory import ScriptFactory
import utilities.logging_manager as lg

def main():
    lg.info("Starting ETL run")
    success = True
    success_registry = set()  # Tracks names of tasks that finished successfully

    try:
        factory = ScriptFactory()
        tasks = factory.get_tasks()

        for task in tasks:
            t_name = task["name"]
            t_func = task["func"]
            t_dep  = task["depends_on"]
            t_enabled = task.get("enabled", True) # Default to True if missing

            # 1. Skip if Disabled
            if not t_enabled:
                lg.info(f"Skipping task '{t_name}': Status is DISABLED")
                continue

            # 2. Check Dependency
            if t_dep and t_dep not in success_registry:
                lg.error(f"Skipping task '{t_name}': Dependency '{t_dep}' failed or was skipped.")
                success = False
                break  # Stop the whole pipeline if a dependency chain breaks

            # 3. Execution
            lg.info(f"Running task: {t_name}")
            try:
                t_func()  # This executes the partial logic
                success_registry.add(t_name)
            except Exception as e:
                lg.error(f"Task '{t_name}' failed: {e}")
                success = False
                break  # Stop the pipeline on error

        if success:
            lg.info("ETL run completed successfully")
        else:
            lg.error("ETL run finished with errors.")

    except Exception as e:
        lg.error(f"Critical error during initialization: {e}")

if __name__ == "__main__":
    main()