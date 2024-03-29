from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
# from airflow.models.baseoperator import chain_linear
# from airflow.models.baseoperator import chain
from datetime import datetime

from typing import Sequence
from airflow.models.taskmixin import DependencyMixin
from airflow.utils.edgemodifier import EdgeModifier
from airflow.exceptions import AirflowException
def chain_linear(*elements):
    """
    Simplify task dependency definition.
            ╭─op2─╮ ╭─op4─╮
        op1─┤     ├─├─op5─┤─op7
            ╰-op3─╯ ╰-op6─╯

    Then you can accomplish like so::

        chain_linear(op1, [op2, op3], [op4, op5, op6], op7)

    :param elements: a list of operators / lists of operators

    Repack by k.melnikov@temabit.com
    """
    if not elements:
        raise ValueError("No tasks provided; nothing to do.")
    prev_elem = None
    deps_set = False
    for curr_elem in elements:
        if prev_elem is not None:
            for task in prev_elem:
                task >> curr_elem
                if not deps_set:
                    deps_set = True
        prev_elem = [curr_elem] if isinstance(curr_elem, DependencyMixin) else curr_elem
    if not deps_set:
        raise ValueError("No dependencies were set. Did you forget to expand with `*`?")
    
def chain(*tasks) -> None:    
    """
    Given a number of tasks, builds a dependency chain.

    This function accepts values of BaseOperator (aka tasks), EdgeModifiers (aka Labels), XComArg, TaskGroups,
    or lists containing any mix of these types (or a mix in the same list). If you want to chain between two
    lists you must ensure they have the same length.

    Repack by k.melnikov@temabit.com
    """
    for up_task, down_task in zip(tasks, tasks[1:]):
        if isinstance(up_task, DependencyMixin):
            up_task.set_downstream(down_task)
            continue
        if isinstance(down_task, DependencyMixin):
            down_task.set_upstream(up_task)
            continue
        if not isinstance(up_task, Sequence) or not isinstance(down_task, Sequence):
            raise TypeError(f"Chain not supported between instances of {type(up_task)} and {type(down_task)}")
        up_task_list = up_task
        down_task_list = down_task
        if len(up_task_list) != len(down_task_list):
            raise AirflowException(
                f"Chain not supported for different length Iterable. "
                f"Got {len(up_task_list)} and {len(down_task_list)}."
            )
        for up_t, down_t in zip(up_task_list, down_task_list):
            up_t.set_downstream(down_t)

def _t1(ti):
    response = 2+2
    ti.xcom_push(key='test_key', value=response)
 
def _t2(ti):
    response = ti.xcom_pull(key='test_key', task_ids='t1')
    print(response)

def _branch(ti):
    value = ti.xcom_pull(key='test_key', task_ids='t1')
    if value == 4:
        return 't2'
    else:
        return 't3'
 
with DAG("xcom_dag_test", start_date=datetime(2022, 1, 1), 
    schedule_interval='@daily', catchup=False) as dag:
    
        
    t1 = PythonOperator(
        task_id='t1',
        python_callable=_t1
    )

    branch = BranchPythonOperator(
        task_id='branch',
        python_callable=_branch
    )
 
    t2 = PythonOperator(
        task_id='t2',
        python_callable=_t2
    )
 
    t3 = BashOperator(
        task_id='t3',
        bash_command="echo ''"
    )
 
    t4 = BashOperator(
        task_id='t4',
        bash_command="echo ''",
        trigger_rule='all_success' # it has 10 rules but by default is 'all_success' 
    )
 
    t5 = BashOperator(
        task_id='t5',
        bash_command="echo ''"
    )
    
    chain(t1, branch, [t2, t3], [t4, t5])
    # chain_linear(t1, branch, [t2, t3], [t4, t5])
    # t1 >> branch >> [t2, t3]