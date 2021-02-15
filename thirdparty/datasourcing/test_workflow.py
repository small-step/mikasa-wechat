import datasourcing

def add():
    print("I am add")

def plus():
    print("I am plus")

def sub():
    print("I am sub")

def min():
    print("I am min")

if __name__ == "__main__":
    graph = datasourcing.workflow.TaskGraph('test_workflow')
    add_task = datasourcing.workflow.PythonTask(
                graph=graph,
                task_id='add',
                python_callable=add,
                op_args=[],
                op_kwargs={})

    plus_task = datasourcing.workflow.PythonTask(
                graph=graph,
                task_id='plus',
                python_callable=plus,
                op_args=[],
                op_kwargs={})

    min_task = datasourcing.workflow.PythonTask(
                graph=graph,
                task_id='min',
                python_callable=min,
                op_args=[],
                op_kwargs={})

    plus_task >> [min_task, add_task]
    graph.run()