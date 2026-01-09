#This is the “engine”.

# It:

# imports ScriptFactory

# gets the list of tasks

# runs them in order

# logs success/failure

# handles exceptions

# triggers email alerts

# This is the “executor”.

from script_factory.script_factory import ScriptFactory
import common.logging as lg

def main():
    factory = ScriptFactory()
    tasks = factory.get_tasks()

    for task in tasks:
        lg.logger.info(f"Running task: {task.__name__}")
        task()

if __name__ == "__main__":
    main()

