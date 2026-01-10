import time
import utilities.logging_manager as lg

from script_factory.script_factory import ScriptFactory

def main():
    lg.info("Starting ETL run")
    success = True
    # success_registry stores the 'task_name' of every task that finished without error.
    # This is used to validate the 'depends_on' requirements.
    success_registry = set()

    try:
        # 1. INITIALIZATION
        # Create the factory instance and fetch the list of task dictionaries
        factory = ScriptFactory()
        tasks = factory.init_tasks()

        for task in tasks:
            # 2. DATA EXTRACTION
            # Match the keys exactly as defined in init_tasks() dictionaries.
            # Using .get() for 'enabled', 'retries' and 'description' provides a fallback value
            # if those keys are missing from a specific dictionary.
            t_name = task["task_name"]              # Maps to task_1["task_name"]
            t_func = task["func"]                   # This is the partial() object
            t_dep = task["depends_on"]              # The name of the required previous task
            t_enabled = task.get("enabled", True)   # Default to True if key is missing
            t_retries = task.get("retries", 0)      # Default to 0 if key is missing
            t_desc = task.get("description", "")    # Get description for logging

            # 3. ENABLED CHECK
            # If a task is explicitly set to False, we log it and move to the next item.
            if not t_enabled:
                lg.info(f"Skipping task '{t_name}': Status is DISABLED")
                continue

            # 4. DEPENDENCY CHECK
            # If 'depends_on' is not None, we check if that task name exists in our success_registry.
            # If the parent task failed or was skipped, this child task cannot run.
            if t_dep and (t_dep not in success_registry):
                lg.error(f"Stopping pipeline: Task '{t_name}' depends on '{t_dep}', but '{t_dep}' was not successful.")
                success = False
                break

            # 5. EXECUTION & RETRY LOOP
            # Define a boolean variable to track the success of a task
            task_passed_finally = False

            # The loop range is (retries + 1).
            # If retries=1, the loop runs for attempt 0 (initial) and attempt 1 (retry).
            for attempt in range(t_retries + 1):
                try:
                    if attempt > 0:
                        lg.info(f"Retrying task '{t_name}'... (Attempt {attempt} of {t_retries})")

                    lg.info(f"Executing: {t_name} - {task['description']}")

                    # Trigger the partial function with all its pre-set arguments
                    t_func()

                    # If we reach this line, the function finished successfully
                    task_passed_finally = True

                    # Mark as success for future dependencies
                    success_registry.add(t_name)

                    # Exit the retry loop early
                    break

                except Exception as e:
                    lg.error(f"Attempt {attempt} failed for '{t_name}': {str(e)}")

                    # If there are still retries left, wait 1 second before trying again
                    if attempt < t_retries:
                        time.sleep(1)
                    else:
                        lg.error(f"Task '{t_name}' exhausted all retry attempts.")

            # 6. PIPELINE HALT
            # If the task failed all retries, 'task_passed_finally' remains False.
            # We stop the entire ETL process to prevent data corruption or inconsistent states in subsequent tasks.
            if not task_passed_finally:
                success = False
                lg.error(f"Pipeline execution halted due to failure in: {t_name}")
                break

        # 7. FINAL STATUS
        if success:
            lg.info("ETL run completed successfully.")
        else:
            lg.error("ETL run finished with errors.")

    except Exception as e:
        lg.error(f"Critical error during factory initialization: {e}")


if __name__ == "__main__":
    main()