from task_logic import *


def create_load_task():
    return task_load

def create_optimize_task():
    return task_optimize

TASK_FUNCTIONS = {
    "load": create_load_task,
    "optimize": create_optimize_task
}