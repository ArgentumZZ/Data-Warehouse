# import libraries
import time, sys, traceback

# import custom libraries
from custom_code.script_factory import ScriptFactory
import custom_code.script_parameters as settings
import utilities.logging_manager as lg
from utilities.argument_parser import parse_arguments
from utilities.email_manager import EmailManager

def main():
    """
    Run script execution algorithm.

    1. Script startup.
        - execute the project via run_script.py.
        - capture the start time of execution.
        - initialize an overall success flag to track final pipeline status
        - create a success_registry set to track tasks that completed successfully and for dependency validation.
    2. Initialization:
        - read command-line arguments using parse_arguments to override internal default settings.
        - initialize the ScriptFactory class using the parsed arguments and configuration settings.
        - initialize the EmailManager class using the factory instance.
        - retrieve the list of task definitions from the factory.
    3. Task iteration.
        - iterate over the list of task dictionaries.
        - for each task, extract the following attributes:
            - task name
            - execution function
            - dependency
            - enabled status
            - number of retries
            - task description
    4, Enabled check.
        - if a task is disabled:
            - log the task as skipped.
            - add the task result and reason to the e-mail report.
            - continue the execution with the next task.
    5. Dependency check.
        - if a task has a dependency and the dependent task is not present in the success_registry set.
            - mark the task as skipped.
            - log dependency failure information and add it to the e-mail.
            - set the overall success flag to False.
            - continue execution with the next task.
    6. Task execution with retries.
        - initialize a task-level success flag.
        - record a log pointer to capture task-specific logs.
        - execute the task function inside a retry loop:
            - on success:
                - mark the task as successful
                - add the task name to the success_registry set.
                - add the success information to the e-mail.
                - exit the retry loop early
            - on failure:
                - capture the exception traceback and log it.
                - if retries remain, sleep for X seconds before trying again.
                - if all retries are exhausted, mark the task as failed in the e-mail report.
    7. Log collection.
        - collect the generated logs after the task execution (successful or not)
        - add task-specific logs to the e-mail report.
    8. Halt pipeline on failure.
        - if a task fails after all retries:
            - set the overall success flag to False.
            - log the pipeline halt message.
            - terminate the main task loop to prevent downstream inconsistencies.
    9. Global exception handling.
        - log any unexcepted exception during pipeline execution
        - set the overall success flag to False.
    10. Final status.
        - log the final pipeline status.
        - calculate and format the total execution duration
        - prepare and send the e-mails
        - perform a log maintenance
        - exit the script with 0 (=success) or 1 (=failure).
    """
    # Capture start time
    start_time = time.time()

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

            t_name = task["task_name"]                   # Name of the task
            t_func = task["function"]                    # This is the partial() object
            t_dep = task["depends_on"]                   # The name of the required previous task (dependency)
            t_enabled = task["is_enabled"]               # Determines if the task is enabled or disabled
            t_retries = task["retries"]                  # The number of retries for each task
            t_desc = task["description"]                 # Description of the task

            # 3. Is enabled check.
            # If a task is explicitly set to False, we log it and move to the next item.
            if not t_enabled:
                lg.info(f"Skipping task '{t_name}': Status is DISABLED")

                # include it in the e-mail as disabled
                email_manager.add_task_result_to_email(task=task, status="DISABLED")

                # include it in the log
                email_manager.add_log_block_to_email(task_name=t_name, logs="Task is disabled in configuration.", task=task)

                continue

            # 4. Dependency check.
            # If 'depends_on' is not None, we check if that task name exists in the success_registry.
            # If the previous task failed or was skipped, this task can't run.
            if t_dep and (t_dep not in success_registry):
                lg.info(f"Stopping pipeline: Task '{t_name}' depends on '{t_dep}', but '{t_dep}' was not successful.")
                error_msg = f"Dependency {t_dep} failed."

                # Append a formatted HTML table row to the `internal task log` section
                email_manager.add_task_result_to_email(task=task, status="SKIPPED", error_msg=error_msg)

                # Capture the error the log created for the `technical log details` section
                email_manager.add_log_block_to_email(task_name=t_name, logs=f"SKIPPED: {error_msg}", task=task)

                success = False
                continue

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

                    # Add the task result to the email
                    email_manager.add_task_result_to_email(task=task, status="SUCCESS")

                    # Exit the retry loop early
                    break

                except Exception as e:
                    lg.info(f"Attempt {attempt} failed for '{t_name}': {str(e)}")

                    # If there are still retries left, wait 5 second before trying again
                    if attempt < t_retries:
                        lg.info("Waiting 5 seconds before next retry...")
                        time.sleep(5)
                    else:
                        lg.info(f"Task '{t_name}' exhausted all retry attempts.")

                        # Mark the task as failed
                        email_manager.add_task_result_to_email(task=task, status="FAILED", error_msg="See Technical Log Details below")

            # Read and return the log content from a specific byte offset to the end
            task_specific_logs = lg.get_logs_from_position(log_start_position)

            # Add the logs to the e-mail
            email_manager.add_log_block_to_email(task_name=t_name, logs=task_specific_logs, task=task)

            # 6. Pipeline halt
            # If the task failed all retries, 'task_passed_finally' remains False.
            # We stop the entire ETL process to prevent data corruption or inconsistent states in subsequent tasks.
            if not task_passed_finally:
                success = False
                lg.info(f"Pipeline execution halted due to failure in: {t_name}")
                break

    except Exception as e:
        lg.info(f"Critical error during execution: {e}")
        success = False

    finally:
        # 7. Log the final status and calculate script time execution
        lg.info("ETL run completed successfully.") if success else lg.info("ETL run finished with errors.")

        # Calculate the total duration of the script
        duration_seconds = time.time() - start_time

        # Reformat into HH:MM:SS
        # time.gmtime() converts a number of seconds into a structured time object
        duration_formatted = time.strftime("%H:%M:%S", time.gmtime(duration_seconds))

        try:
            # 8. Prepare the mails
            email_manager.prepare_emails(script_execution_time=duration_formatted)

            # 9. Send the mails
            email_manager.send_emails(is_error=not success)
        except Exception as email_error:
            lg.info(f"Failed to send emails: {email_error}")

        try:
            # 10. Log maintenance
            lg.info("Performing log maintenance...")
            lg.cleanup_old_logs(
                                log_dir=lg.log_dir,
                                retention_number=factory.log_retention_number,
                                is_enabled=factory.delete_log,
                                mode=factory.log_mode
                                )
        except Exception as maintenance_error:
            print(f"Log maintenance failed: {maintenance_error}")

        # 11. Final system exit
        # sys.exit(0)  # Explicit success
        # sys.exit(1)  # Explicit crash
        sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()