from ingest_dag_logic import task_load, task_optimize


# class BuilderFactory:

# @classmethod
def create_load_task(**kwargs):
    task_load()

# @classmethod
def create_optimize_task(**kwargs):
    task_optimize()

TASK_FUNCTIONS = {
    "load": task_load,
    "optimize": task_optimize
}
