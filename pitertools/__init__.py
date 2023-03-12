from typing import Callable, Iterator, Tuple, TypeVar
from threading import Event, Lock, Thread
from queue import Queue


T = TypeVar("T")
G = TypeVar("G")


def map_parallel(
    func: Callable[[T], G],
    it: Iterator[T],
    num_workers: int,
    ordered = False,
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
    enumerated_it = enumerate(it)
    for i in range(num_workers):
        stop_event = Event()
        t = Thread(
            target=_task,
            kwargs={
                'it': enumerated_it,
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
    current_index = 0
    stored_items = []
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
        item_index = current[0]
        item = current[1]
        if ordered:
            if item_index == current_index:
                yield item
                current_index += 1
                while True:
                    if not stored_items:
                        break
                    if stored_items[-1][0] == current_index:
                        current_item = stored_items.pop()[1]
                        yield current_item
                        current_index += 1
                    else:
                        break
            else:
                _insort(stored_items, current, key=lambda current: -current[0])
        else:
            yield item

def _insort(a, x, key):
    lo = _bisect_right(a, x, key=key)
    a.insert(lo, x)

def _bisect_right(a, x, key) -> int:
    """Return the index where to insert item x in list a, assuming a is sorted.

    The return value i is such that all e in a[:i] have e <= x, and all e in
    a[i:] have e > x.  So if x already appears in the list, a.insert(x) will
    insert just after the rightmost x already there.

    """
    lo = 0
    hi = len(a)
    while lo < hi:
        mid = (lo+hi)//2
        if key(x) < key(a[mid]): hi = mid
        else: lo = mid+1
    return lo


def _task(
    it: Iterator[Tuple[int,T]],
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
        except StopIteration:
            if verbose:
                print(f"Worker {i} finished")
            results_queue.put(_End(), block=True)
            break
        finally:
            lock.release()
        try:
            transformed = func(current[1])
        except Exception as e:
            if verbose:
                print(f"worker {i} failed with exception {e}")
            results_queue.put(_Error(e), block=True)
            break
        results_queue.put((current[0], transformed), block=True)


class _Error:
    def __init__(self, e: Exception) -> None:
        self.e = e


class _End:
    pass
