from task_logic import TaskLogic

class TaskFactory:
    @staticmethod
    def create_load_task():
        return TaskLogic.task_load
    @staticmethod
    def create_optimize_task():
        return TaskLogic.task_optimize

    TASK_FUNCTIONS = {
        "load": create_load_task,
        "optimize": create_optimize_task
    }