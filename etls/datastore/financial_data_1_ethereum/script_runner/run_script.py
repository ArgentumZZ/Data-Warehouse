# import libraries
import time, sys

# import custom libraries
from custom_code.script_factory import ScriptFactory
import custom_code.script_parameters as settings
import utilities.logging_manager as lg
from utilities.argument_parser import parse_arguments
from utilities.email_manager import EmailManager

def main():
    lg.info("Starting the ETL run")
    success = True
    # success_registry stores the 'task_name' of every task that finished without error.
    # This is used to validate the 'depends_on' requirements.
    success_registry = set()

    # 1. Initialization
    # Parse the .bat/.sh file for input parameters
    forced_sdt, load_type, max_days_to_load = parse_arguments(settings)

    # Initialize ScriptFactory
    factory = ScriptFactory(
        forced_sdt=forced_sdt,
        load_type=load_type,
        max_days_to_load=max_days_to_load,
        settings=settings
    )

    # Initialize EmailManager
    email_manager = EmailManager(factory=factory)

    # Fetch the list of task dictionaries
    tasks = factory.init_tasks()

    try:
        for task in tasks:
            # 2. Data extraction
            # Match the keys exactly as defined in init_tasks() dictionaries.
            # Using .get() for 'enabled', 'retries' and 'description' provide a fallback value,
            # if those keys are missing from a specific dictionary.

            t_name = task["task_name"]                 # Name of the task
            t_func = task["function"]                  # This is the partial() object
            t_dep = task["depends_on"]                 # The name of the required previous task
            t_enabled = task.get("is_enabled", True)   # Default to True if key is missing
            t_retries = task.get("retries", 0)         # Default to 0 if key is missing
            t_desc = task.get("description", "")       # Get description for logging

            # 3. Is enabled check.
            # If a task is explicitly set to False, we log it and move to the next item.
            if not t_enabled:
                lg.info(f"Skipping task '{t_name}': Status is DISABLED")
                continue

            # 4. Dependency check.
            # If 'depends_on' is not None, we check if that task name exists in our success_registry.
            # If the parent task failed or was skipped, this child task cannot run.
            if t_dep and (t_dep not in success_registry):
                lg.error(f"Stopping pipeline: Task '{t_name}' depends on '{t_dep}', but '{t_dep}' was not successful.")
                error_msg = f"Dependency {t_dep} failed."

                # Append a formatted HTML table row to the `internal task log` string
                email_manager.add_task_result_to_email(task=task, status="SKIPPED", error_msg=error_msg)

                # Captures the error log created for the `technical log details` section
                email_manager.add_log_block_to_email(t_name, f"SKIPPED: {error_msg}")

                success = False
                break

            # 5. Execution and retry loop
            # Define a boolean variable to track the success of a task
            task_passed_finally = False

            # a pointer for the start of a task
            log_start_position = lg.get_current_log_size()

            # If retries=1, the loop runs for attempt 0 (initial) and attempt 1 (retry).
            for attempt in range(0, t_retries + 1):
                try:
                    if attempt > 0:
                        lg.info(f"Retrying task '{t_name}'... (Attempt {attempt} of {t_retries})")

                    lg.info(f"Executing: {t_name} - {t_desc}")

                    # Trigger the partial function with all its pre-set arguments
                    t_func()

                    # If we reach this line, the function finished successfully
                    task_passed_finally = True

                    # Mark as success for future dependencies
                    success_registry.add(t_name)

                    # add task result to email
                    email_manager.add_task_result_to_email(task=task, status="SUCCESS")

                    # Exit the retry loop early
                    break

                except Exception as e:
                    lg.error(f"Attempt {attempt} failed for '{t_name}': {str(e)}")

                    # If there are still retries left, wait 5 second before trying again
                    if attempt < t_retries:
                        lg.info("Waiting 5 seconds before next retry...")
                        time.sleep(5)
                    else:
                        lg.error(f"Task '{t_name}' exhausted all retry attempts.")

                        email_manager.add_task_result_to_email(task=task, status="FAILED", error_msg=e)

            # Read and return the log content from a specific byte offset to the end
            task_specific_logs = lg.get_logs_from_position(log_start_position)

            # Add the logs to the e-mail
            email_manager.add_log_block_to_email(t_name, task_specific_logs)

            # 6. Pipeline halt
            # If the task failed all retries, 'task_passed_finally' remains False.
            # We stop the entire ETL process to prevent data corruption or inconsistent states in subsequent tasks.
            if not task_passed_finally:
                success = False
                lg.error(f"Pipeline execution halted due to failure in: {t_name}")
                break

    except Exception as e:
        lg.error(f"Critical error during execution: {e}")
        success = False

    finally:
        # 7. Log the final status
        if success:
            lg.info("ETL run completed successfully.")
        else:
            lg.error("ETL run finished with errors.")

        try:
            # 8. Prepare the mails
            email_manager.prepare_mails()

            # 9. Send the mails
            email_manager.send_mails(is_error=not success)
        except Exception as email_error:
            lg.info(f"Failed to send emails: {email_error}")

        try:
            # 10. Log maintenance
            lg.info("Performing log maintenance...")
            lg.cleanup_old_logs(
                                log_dir=lg.log_dir,
                                retention_number=5,
                                is_enabled=factory.delete_log,
                                mode='N'
                                )
        except Exception as maintenance_error:
            print(f"Log maintenance failed: {maintenance_error}")

        # 11. Final system exit
        # sys.exit(0)  # Explicit success
        # sys.exit(1)  # Explicit crash
        sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()