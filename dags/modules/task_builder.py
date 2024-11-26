from scripts.ingest_dag_logic import task_load, task_optimize


# class BuilderFactory:

# @classmethod
def create_load_task(**kwargs):
    return task_load()

# @classmethod
def create_optimize_task(**kwargs):
    return task_optimize()

TASK_FUNCTIONS = {
    "load": create_load_task,
    "optimize": create_optimize_task
}
