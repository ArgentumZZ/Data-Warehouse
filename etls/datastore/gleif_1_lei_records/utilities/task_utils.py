"""
task_utils.py

Purpose:
    Provide a simple task and task runner system for ETL pipelines.
    Supports:
        - Task definition
        - Dependencies
        - Sequential execution
"""

class Task:
    """Represents a single ETL task."""

    def __init__(self, func, name, depends_on=None):
        """
        Initialize a task.

        Args:
            func (callable): Task function.
            name (str): Task name.
            depends_on (list[str]): Names of tasks this one depends on.
        """
        self.func = func
        self.name = name
        self.depends_on = depends_on or []


class TaskRunner:
    """Executes tasks in order."""

    def __init__(self):
        self.tasks = []

    def add(self, task):
        """
        Add a task to the execution list.

        Args:
            task (Task): Task instance.
        """
        self.tasks.append(task)

    def run(self):
        """
        Execute all tasks in order.
        """
        for task in self.tasks:
            task.func()
