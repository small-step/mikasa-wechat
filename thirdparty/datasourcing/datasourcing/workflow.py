from typing import (
    Any, Callable, ClassVar, Dict, FrozenSet, Iterable, List, Optional, Set, Tuple, Type, Union, cast,
)
from functools import partial
import queue
import time
import multiprocessing
import datetime
import subprocess
import logging
import copy

def run_task(task:'Task'):
    task.run()

class Task(object):
    def __init__(self,
            task_id:str,
            graph:'TaskGraph',
        ):
        self.task_id = task_id
        self.graph = graph
        self.graph.add(self)
    
    def __call__(self):
        self.run()

    def run(self):
        raise NotImplementedError("Task.run not implemented")

    def set_upstream(self, upstream:Union['Task', Iterable['Task']]):
        if isinstance(upstream, Iterable):
            for each_upstream in upstream:
                self.graph.set_downstream(each_upstream, self)
        else:
            self.graph.set_downstream(upstream, self)

    def set_downstream(self, downstream:Union['Task', Iterable['Task']]):
        if isinstance(downstream, Iterable):
            for each_downstream in downstream:
                self.graph.set_downstream(self, each_downstream)
        else:
            self.graph.set_downstream(self, downstream)

    def __rshift__(self, other):
        """
        Implements Self >> Other == self.set_downstream(other)
        """
        self.set_downstream(other)
        return other

    def __lshift__(self, other):
        """
        Implements Self << Other == self.set_upstream(other)
        """
        self.set_upstream(other)
        return other

    def __rrshift__(self, other):
        """
        Called for Operator >> [Operator] because list don't have
        __rshift__ operators.
        """
        self.__lshift__(other)
        return self

    def __rlshift__(self, other):
        """
        Called for Operator << [Operator] because list don't have
        __lshift__ operators.
        """
        self.__rshift__(other)
        return self

class BashTask(Task):
    def __init__(self,
            task_id:str,
            graph:'TaskGraph',
            bash_command:str,
            params:Optional[List]=[]
        ):
        super(BashTask, self).__init__(task_id=task_id, graph=graph)
        self.bash_command = bash_command
        self.params = params

    def run(self):
        p = []
        p.append(self.bash_command)
        p.extend(self.params)
        subprocess.run(p)

class PythonTask(Task):
    def __init__(self,
            task_id:str,
            graph:'TaskGraph',
            python_callable:Callable,
            op_args:Optional[List]=[],
            op_kwargs:Optional[Dict]={},
        ):
        super(PythonTask, self).__init__(task_id=task_id, graph=graph)
        if not callable(python_callable):
            raise Exception('python_callable param must be callable')
        self.callable = python_callable
        self.op_args = op_args
        self.op_kwargs = op_kwargs
    def run(self):
        self.callable(*self.op_args, **self.op_kwargs)

class Parameter(object):
    def __init__(self):
        pass


class Target(object):
    def __init__(self):
        pass

class TaskGraph(object):
    def __init__(self,
            name:str,
            description:str = '',
            schedule_interval:datetime.timedelta=datetime.timedelta(days=1),
            tags:list=[],
            task_concurrency:int=4
        ):
        self.tasks = {}
        # downstreams
        self.deps = {}
        self.task_concurrency = task_concurrency

    def add(self, task:Task):
        if task.task_id in self.tasks:
            raise Exception("This task id is duplicated")
        self.tasks[task.task_id] = task
        self.deps[task.task_id] = []

    def set_downstream(self, task:Task, deps:Union[Task, Iterable[Task]]):
        if isinstance(deps, Iterable):
            if task.task_id in self.deps:
                self.deps[task.task_id].extend([each_dep.task_id for each_dep in deps])
            else:
                self.deps[task.task_id] = [each_dep.task_id for each_dep in deps]
        else:
            if task.task_id in self.deps:
                self.deps[task.task_id].append(deps.task_id)
            else:
                self.deps[task.task_id] = [deps.task_id]

    def _is_dag(self) -> bool:
        # 入度
        reverse_deps = {}
        for each_task_id in self.tasks.keys():
            # 保证每个节点都创建set
            if each_task_id not in reverse_deps:
                reverse_deps[each_task_id] = set()
            # 将依赖的出度转换为入度
            for each_dep in self.deps[each_task_id]:
                if each_dep not in reverse_deps:
                    reverse_deps[each_dep] = set()
                reverse_deps[each_dep].add(each_task_id)
        
        deleted_node_set = set()
        result = []
        have_in_zero = True
        while have_in_zero:
            have_in_zero = False
            for each_task_id in self.tasks.keys():
                # 找入度为0的节点
                if each_task_id not in deleted_node_set and len(reverse_deps[each_task_id])==0:
                    # 删除该节点
                    deleted_node_set.add(each_task_id)
                    for each_tmp_task_id in self.tasks.keys():
                        if each_task_id in reverse_deps[each_tmp_task_id]:
                            reverse_deps[each_tmp_task_id].remove(each_task_id)
                    # 添加到结果
                    result.append(each_task_id)
                    have_in_zero = True
                    break
        return len(result) == len(self.tasks)


    def run(self):
        if not self._is_dag():
            logging.error("This TaskGraph is not DAG")
            raise Exception("This TaskGraph is not DAG")

        # 入度
        reverse_deps = {}
        for each_task_id in self.tasks.keys():
            # 保证每个节点都创建set
            if each_task_id not in reverse_deps:
                reverse_deps[each_task_id] = set()
            # 将依赖的出度转换为入度
            for each_dep in self.deps[each_task_id]:
                if each_dep not in reverse_deps:
                    reverse_deps[each_dep] = set()
                reverse_deps[each_dep].add(each_task_id)

        pool = {}
        task_queue = queue.SimpleQueue()

        finished_tasks = set()
        def get_available_tasks():
            result = []
            have_in_zero = True
            while have_in_zero:
                have_in_zero = False
                for each_task_id in self.tasks.keys():
                    # 找入度为0的节点
                    if each_task_id not in finished_tasks and \
                        each_task_id not in pool.keys() and \
                        each_task_id not in result and \
                        len(reverse_deps[each_task_id])==0:
                        # 添加到结果
                        result.append(each_task_id)
                        have_in_zero = True
                        break
            return result

        while len(finished_tasks) != len(self.tasks):
            tasks_need_deleted = []
            for task_id, each_p in pool.items():
                # 找出已经结束的进程
                if not each_p.is_alive():
                    if each_p.exitcode < 0:
                        logging.warning(f"Task {task_id} exit with {each_p.exitcode}")
                    tasks_need_deleted.append(task_id)
            # 删除旧进程
            for each_task in tasks_need_deleted:
                logging.info(f"Task {each_task} finished")
                pool.pop(each_task)
                finished_tasks.add(each_task)

                # 入度表中删除该节点
                for each_task_id in self.tasks.keys():
                    if each_task in reverse_deps[each_task_id]:
                        reverse_deps[each_task_id].remove(each_task)
            # 搜索目前可能执行的任务
            result = get_available_tasks()
            for each_task in result:
                task_queue.put(each_task)
            # 启动新进程
            # 先查看要启动多少新进程
            new_num = self.task_concurrency - len(pool)
            for _ in range(new_num):
                if task_queue.empty():
                    break
                new_task = task_queue.get()
                p = multiprocessing.Process(target=self.tasks[new_task].run)
                pool[new_task] = p
                logging.info(f"Task {new_task} started")
                p.start()

            time.sleep(0)
                



