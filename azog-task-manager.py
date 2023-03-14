import os
from subprocess import Popen
import sys
import pickle
from io import StringIO
import traceback
import time
from collections import Counter
from uuid import uuid4

TASK_VARIABLE_NAME = "PIPELINE_TASK_NAME"
TASK_ARGUMENTS_PATH = "PIPELINE_TASK_PATH"
TASK_REGISTRY = {}


def in_task_mode():
    return TASK_VARIABLE_NAME in os.environ


def with_default(value, default):
    return value if value is not None else default
    

class ParallelTaskException(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message


class Promise:
    def __init__(self, name, subprocess_args, *args, **kwargs):
        env = dict(os.environ)

        env[TASK_VARIABLE_NAME] = name

        args_path = os.path.join("/tmp", str(uuid4()).replace("-", "_"))
        assert not os.path.exists(args_path)
        with open(args_path, "wb") as f:
            pickle.dump((args, kwargs), f)
        env[TASK_ARGUMENTS_PATH] = args_path

        process = Popen(
            subprocess_args, 
            env=env,
            universal_newlines=False, 
            start_new_session=True
        )

        self.name = name
        self.process = process
        self.args_file_path = args_path
        self.args_file_deleted = False

    def _clear_args_file(self):
        if not self.args_file_deleted:
            os.remove(self.args_file_path)
            self.args_file_deleted = True

    def wait(self):
        try:
            self.process.wait()
            
            if self.process.returncode != 0:
                raise ParallelTaskException(f"Parallel task {self.name} failed")
            
            with open(self.args_file_path, "rb") as f:
                result = pickle.load(f)
            
            if isinstance(result, ParallelTaskException):
                raise Exception(
                    f"Parallel task {self.name} failed. Error: \n {result.message}\n")
            
            return result
        finally:
            self._clear_args_file()

    def __del__(self):
        if not self.args_file_deleted:
            print(f"WARNING: PROMISE HAS BEEN DELETED BEFORE TASK {self.name} FINISHED. IT'S PROBABLY AN ERROR. TERMINATING TASK.")
            self.process.terminate()
            self._clear_args_file()


class ParallelTask:
    def __init__(self, f, n_cores, memory):
        self.f = f
        self.n_cores = n_cores
        self.memory = memory
        
    def with_parameters(self, n_cores=None, memory=None):
        return ParallelTask(
            self.f, 
            with_default(n_cores, self.n_cores),
            with_default(memory, self.memory),
        )
    
    def __call__(self, *args, **kwargs):
        if in_task_mode():
            return self.task_mode_call(*args, **kwargs)
        else:
            return self.main_mode_call(*args, **kwargs)

    @property
    def name(self):
        return self.f.__name__

    def main_mode_call(self, *args, **kwargs):
        subprocess_args = []
        if self.n_cores > 0:
            assert self.memory is not None, "When n_cores > 0, memory should be defined"
            memory_in_mb = int(self.memory / 2**20)
            subprocess_args.extend([
                "srun", 
                "--job-name=pavlova",
                f"--cpus-per-task={self.n_cores}",
                f"--mem={memory_in_mb}",
                "--ntasks=1"
            ])
        subprocess_args.extend([sys.executable, sys.argv[0]])
        promise = Promise(self.name, subprocess_args, *args, **kwargs)

        return promise

    def task_mode_call(self):
        with open(os.environ[TASK_ARGUMENTS_PATH], "rb") as f:
            args, kwargs = pickle.load(f)

        try:
            result = self.f(*args, **kwargs)
        except:
            exception_error = traceback.format_exc()
            result = ParallelTaskException(exception_error)

        with open(os.environ[TASK_ARGUMENTS_PATH], "wb") as f:
            pickle.dump(result, f)


def parallel_task(n_cores=-1, memory=None):
    def parallel_task_decorator(f):
        parallel_task = ParallelTask(f, n_cores, memory)
        assert f.__name__ not in TASK_REGISTRY
        TASK_REGISTRY[f.__name__] = parallel_task
        return parallel_task
    return parallel_task_decorator


def wait_tasks(tasks, print_time = False):
    start = time.time()
    results =  [task.wait() for task in tasks]
    end = time.time()

    if print_time:
        tasks_names = [task.name for task in tasks]
        names_counts = Counter(tasks_names)
        tasks_names_folded = [f"'{name} x{count}'" for name, count in names_counts.items()]
        tasks_names_str = ", ".join(tasks_names_folded)
        minutes = (end-start) / 60
        print(f"{tasks_names_str} finished in {minutes:.3f} minutes")

    return results


def task_manager_start(main):
    if in_task_mode():
        task_name = os.environ[TASK_VARIABLE_NAME]
        TASK_REGISTRY[task_name]()
    else:
        main()
