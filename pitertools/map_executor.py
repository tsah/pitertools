from concurrent.futures.thread import ThreadPoolExecutor
from queue import Full, Queue
from threading import Event, Lock
from typing import Any, Callable, Dict, Generic, Iterator, List, Optional, Tuple, TypeVar, Union

from pitertools.map_parallel import ParallelMap


T = TypeVar('T')
G = TypeVar('G')


class _Error:
    def __init__(self, e: Exception) -> None:
        self.e = e


class _End:
    pass


class ExecutorShutdownError(Exception):
    pass


def map_parallel_threadpool(
    executor: ThreadPoolExecutor,
    func: Callable[[T], G],
    it: Iterator[T],
    concurrency: int,
    ordered = False,
    verbose: bool = False
) -> ParallelMap[G]:
    """
    Concurrently runs computation on iterator, on given executor
    (ThreadPoolExecutor, ProcessPoolExecutor).
    The implemetation uses backpressure - New tasks are not submitted until previous produced
    values are consumed from the result iterator. Note that sending the results downstream uses
    busy wait while blocking for 1 second each time, so it takes from threads capacity. There will
    not be starvation as new tasks are submitted to the executor for each iteration.
    If any tasks experiences an exception, all running tasks finish gracefully and no new ones
    are submitted.
    """
    return _ParallelMapExecutor(executor, func, it, concurrency, ordered, verbose)


class _ParallelMapExecutor(Generic[T, G]):
    def __init__(
        self,
        executor: ThreadPoolExecutor,
        func: Callable[[T], G],
        it: Iterator[T],
        concurrency: int,
        ordered: bool,
        verbose: bool
    ) -> None:
        # params
        self.executor = executor
        self.func = func
        self.it = enumerate(it)
        self.concurrency = concurrency
        self.ordered = ordered
        self.verbose = verbose
        # state
        self.finished_workers: int = 0
        self.error: Optional[Exception] = None
        self.current_index: int = 0
        self.stored_items: List[Any] = []
        self.lock = Lock()
        self.results_queue: Queue[Union[Tuple[int, G], _Error, _End, ExecutorShutdownError]] = Queue(maxsize=concurrency)
        self.thread_stop_events: List[Event] = []

    def start(self) -> Iterator[G]:
        result_iter = self._iter()
        self.result_iter = result_iter
        return result_iter

    def stop(self) -> None:
        if self.verbose:
            print('Stopping')
        self._stop_all_tasks()
        self._drain_iter()

    def _iter(self) -> Iterator[G]:
        self._start_tasks()
        while True:
            if self._is_finished():
                self._raise_if_needed()
                break
            current = self.results_queue.get(block=True)
            if isinstance(current, _End):
                self.finished_workers += 1
            elif isinstance(current, _Error):
                self._error(current.e)
            elif isinstance(current, ExecutorShutdownError):
                self._error(current)
            else:
                if self.ordered:
                    self._store(current)  # type: ignore[arg-type]
                    yield from self._flush_stored()
                else:
                    yield current[1]  # type: ignore[index, assignment]

    def _start_tasks(self) -> None:
        for i in range(self.concurrency):
            stop_event = Event()
            task = _Task(
                it=self.it,
                func=self.func,
                lock=self.lock,
                t_id=i,
                verbose=self.verbose,
                results_queue=self.results_queue,
                stop_event=stop_event,
                executor=self.executor
            )
            try:
                self.executor.submit(task.run)
            except RuntimeError as e:
                raise ExecutorShutdownError from e
            self.thread_stop_events.append(stop_event)

    def _stop_all_tasks(self) -> None:
        if self.verbose:
            print("Stopping all workers")
        for event in self.thread_stop_events:
            event.set()

    def _drain_iter(self) -> None:
        if self.verbose:
            print("Draining remaining results")
        for _ in self.result_iter:
            pass

    def _is_finished(self) -> bool:
        return self.concurrency == self.finished_workers

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


class _Task(Generic[T, G]):
    
    def __init__(
        self,
        executor: ThreadPoolExecutor,
        it: Iterator[Tuple[int,T]],
        func: Callable[[T], G],
        lock: Lock,
        t_id: int,
        verbose: bool,
        results_queue: Queue,
        stop_event: Event
    ) -> None:
        self.executor = executor
        self.it = it
        self.func = func
        self.lock = lock
        self.t_id = t_id
        self.verbose = verbose
        self.results_queue = results_queue
        self.stop_event = stop_event
        
    def run(self) -> None:
        self._print(f"Worker {self.t_id} is starting")
        if self.stop_event.is_set():
            self._print(f"Worker {self.t_id} stopped")
            self.send_and_terminate(_End(), reschedule_run=False)
            return
        self.lock.acquire(blocking=True)
        try:
            current = next(self.it)
        except StopIteration:
            self._print(f"Worker {self.t_id} finished")
            self.send_and_terminate(_End(), reschedule_run=False)
            return
        finally:
            self.lock.release()
        try:
            transformed = self.func(current[1])
        except Exception as e:
            self._print(f"worker {self.t_id} failed with exception {self.t_id}")
            self.send_and_terminate(_Error(e), reschedule_run=False)
            return
        self.send_and_terminate((current[0], transformed))

    def _print(self, msg: str) -> None:
        if self.verbose:
            print(msg)

    def send_and_terminate(
        self,
        item: Union[Tuple[int, G], _Error, _End],
        reschedule_run: bool = True
    ) -> None:
        try:
            self.results_queue.put(item, block=True, timeout=1)
            if reschedule_run:
                self._submit_or_fail(self.run)
        except Full:
            self.executor.submit(
                self.send_and_terminate,
                item=item,
                reschedule_run=reschedule_run
            )

    def _submit_or_fail(self, task: Callable[..., Any], **kwargs: Dict[str, Any]) -> None:
        try:
            self.executor.submit(task, **kwargs)
        except RuntimeError:
            self.results_queue.put(ExecutorShutdownError(), block=True)
