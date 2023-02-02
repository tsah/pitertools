from typing import Callable, Iterator, TypeVar
from threading import Event, Lock, Thread
from queue import Queue


T = TypeVar("T")
G = TypeVar("G")


def map_parallel(
    func: Callable[[T], G],
    it: Iterator[T],
    num_workers: int,
    verbose: bool = False
) -> Iterator[G]:
    """
    Spins up `num_workers` threads that pull from the iterator 
    and perform the `func` operation in parallel.
    The implemetation uses backpressure - the thread will wait for the produced value to 
    be consumed before pulling a new input value.
    The threads shutdown gracefully when `it` is consumed.
    If any thread experiences an exception, all threads shutdown gracefully after finishing
    current computation.
    """
    lock = Lock()
    results_queue = Queue(maxsize=num_workers)
    thread_stop_events = []
    for i in range(num_workers):
        stop_event = Event()
        t = Thread(
            target=_task,
            kwargs={
                'it': it,
                'func': func,
                'lock': lock,
                'i': i,
                'verbose': verbose,
                'results_queue': results_queue,
                'stop_event': stop_event
            }
        )
        t.start() 
        thread_stop_events.append(stop_event)
    finished = 0
    error = None
    while True:
        if finished == num_workers:
            if error:
                raise error
            else:
                break
        current = results_queue.get(block=True)
        if type(current) == _End:
            finished += 1
            continue
        if type(current) == _Error:
            finished += 1
            error = current.e
            for event in thread_stop_events:
                event.set()
            continue
        yield current


def _task(
    it: Iterator[T],
    func: Callable[[T], G],
    lock: Lock,
    i: int,
    verbose: bool,
    results_queue: Queue,
    stop_event: Event
) -> None:
    if verbose:
        print(f"Worker {i} is starting")
    while True:
        if stop_event.is_set():
            print(f"Worker {i} Stopped")
            results_queue.put(_End(), block=True)
        lock.acquire(blocking=True)
        try:
            current = next(it)
            transformed = func(current)
            results_queue.put(transformed, block=True)
        except StopIteration:
            if verbose:
                print(f"Worker {i} finished")
            results_queue.put(_End(), block=True)
            break
        except Exception as e:
            if verbose:
                print(f"worker {i} failed with exception {e}")
            results_queue.put(_Error(e), block=True)
            break
        finally:
            lock.release()


class _Error:
    def __init__(self, e: Exception) -> None:
        self.e = e


class _End:
    pass
