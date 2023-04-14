from concurrent.futures.thread import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Callable, Iterator, Optional, TypeVar

from pitertools.map_theaded import map_parallel_threaded
from pitertools.map_executor import map_parallel_threadpool


T = TypeVar('T')
G = TypeVar('G')


@contextmanager
def map_parallel(
    func: Callable[[T], G],
    it: Iterator[T],
    concurrency: int,
    ordered = False,
    verbose: bool = False,
    executor: Optional[ThreadPoolExecutor] = None
) -> Iterator[G]:
    """
    Concurrently pulls items from `it` to run `func` on it, with respect to backpresure - new 
    items are pulled from `it` only after results are consumed from the result iterator.
    """
    res = None
    if executor:
        res = map_parallel_threadpool(
            executor=executor,
            func=func,
            it=it,
            concurrency=concurrency,
            ordered=ordered,
            verbose=verbose
        )
    else:
        res = map_parallel_threaded(
            func=func,
            it=it,
            num_workers=concurrency,
            ordered=ordered,
            verbose=verbose
        )
    try:
        yield res.start()
    finally:
        res.stop()
