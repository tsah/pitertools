from typing import Any, Callable, Generic, Iterator, List, Optional, Tuple, TypeVar, Union
from threading import Event, Lock, Thread
from queue import Queue

from pitertools.map_parallel import ParallelMap


T = TypeVar("T")
G = TypeVar("G")


def map_parallel_threaded(
    func: Callable[[T], G],
    it: Iterator[T],
    num_workers: int,
    ordered = False,
    verbose: bool = False
) -> ParallelMap[G]:
    """
    Spins up `num_workers` threads that pull from the iterator 
    and perform the `func` operation in parallel.
    The implemetation uses backpressure - the thread will wait for the produced value to 
    be consumed before pulling a new input value.
    The threads shutdown gracefully when `it` is consumed.
    If any thread experiences an exception, all threads shutdown gracefully after finishing
    current computation.
    """
    return _ParallelMap(func, it, num_workers, ordered, verbose)


class _ParallelMap(Generic[T, G]):
    def __init__(
        self,
        func: Callable[[T], G],
        it: Iterator[T],
        num_workers: int,
        ordered: bool,
        verbose: bool
    ) -> None:
        # params
        self.func = func
        self.it = enumerate(it)
        self.num_workers = num_workers
        self.ordered = ordered
        self.verbose = verbose
        # state
        self.finished_workers: int = 0
        self.error: Optional[Exception] = None
        self.current_index: int = 0
        self.stored_items: List[Any] = []
        self.lock = Lock()
        self.results_queue: Queue[Union[Tuple[int, G], _Error, _End]] = Queue(maxsize=num_workers)
        self.thread_stop_events: List[Event] = []

    def start(self) -> Iterator[G]:
        result_iter = self._iter()
        self.result_iter = result_iter
        return result_iter

    def _iter(self) -> Iterator[G]:
        self._start_workers()
        while True:
            if self._is_finished():
                self._raise_if_needed()
                break
            current = self.results_queue.get(block=True)
            if isinstance(current, _End):
                self.finished_workers += 1
            elif isinstance(current, _Error):
                self._error(current.e)
            else:
                if self.ordered:
                    self._store(current)  # type: ignore[arg-type]
                    yield from self._flush_stored()
                else:
                    yield current[1]  # type: ignore[index, assignment]

    def stop(self) -> None:
        if self.verbose:
            print("Stopping")
        self._stop_all_tasks()
        self._drain_iterator()

    def _start_workers(self) -> None:
        for i in range(self.num_workers):
            stop_event = Event()
            task = _Worker(
                it=self.it,
                func=self.func,
                lock=self.lock,
                t_id=i,
                verbose=self.verbose,
                results_queue=self.results_queue,
                stop_event=stop_event
            )
            thread = Thread(
                target=task.run
            )
            thread.start() 
            self.thread_stop_events.append(stop_event)

    def _stop_all_tasks(self) -> None:
        if self.verbose:
            print("Stopping all tasks")
        for event in self.thread_stop_events:
            event.set()

    def _drain_iterator(self) -> None:
        if self.verbose:
            print("Draining remaining results")
        for _ in self.result_iter:
            pass

    def _is_finished(self) -> bool:
        return self.num_workers == self.finished_workers

    def _raise_if_needed(self) -> None:
        if self.error:
            raise self.error

    def _flush_stored(self) -> List[G]:
        res: List[G] = []
        while True:
            if not self.stored_items:
                return res
            if self.stored_items[-1][0] == self.current_index:
                current_item = self.stored_items.pop()[1]
                res.append(current_item)
                self.current_index += 1
            else:
                return res

    def _store(self, item: Tuple[int, G]) -> None:
        _insort(self.stored_items, item, key=lambda item: -item[0])

    def _error(self, e: Exception) -> None:
        self.finished_workers += 1
        self.error = e
        self._stop_all_tasks()


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
        if key(x) < key(a[mid]):
            hi = mid
        else:
            lo = mid+1
    return lo


class _Worker(Generic[T, G]):
    
    def __init__(
        self,
        it: Iterator[Tuple[int,T]],
        func: Callable[[T], G],
        lock: Lock,
        t_id: int,
        verbose: bool,
        results_queue: Queue,
        stop_event: Event
    ) -> None:
        self.it = it
        self.func = func
        self.lock = lock
        self.t_id = t_id
        self.verbose = verbose
        self.results_queue = results_queue
        self.stop_event = stop_event
        
    def run(self) -> None:
        self._print(f"Worker {self.t_id} is starting")
        while True:
            if self.stop_event.is_set():
                self._print(f"Worker {self.t_id} stopped")
                self._send(_End())
                break
            self.lock.acquire(blocking=True)
            try:
                current = next(self.it)
            except StopIteration:
                self._print(f"Worker {self.t_id} finished")
                self._send(_End())
                break
            finally:
                self.lock.release()
            try:
                transformed = self.func(current[1])
            except Exception as e:
                self._print(f"worker {self.t_id} failed with exception {self.t_id}")
                self._send(_Error(e))
                break
            self._send((current[0], transformed))

    def _print(self, msg: str) -> None:
        if self.verbose:
            print(msg)

    def _send(self, item: Any) -> None:
        self.results_queue.put(item, block=True)


class _Error:
    def __init__(self, e: Exception) -> None:
        self.e = e


class _End:
    pass
