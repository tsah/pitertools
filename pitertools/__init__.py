from collections.abc import Iterator
from concurrent.futures.thread import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Callable, TypeVar

from pitertools.map_threaded import map_parallel_threaded
from pitertools.map_executor import map_parallel_threadpool
# from pitertools.map_async import map_parallel_async
from pitertools.map_parallel import ParallelMap


T = TypeVar('T')
G = TypeVar('G')


@contextmanager
def map_parallel(
    func: Callable[[T], G],
    it: Iterator[T],
    concurrency: int,
    ordered = False,
    verbose: bool = False,
    executor: ThreadPoolExecutor | None = None
) -> Iterator[G]:
    """
    Concurrently pulls items from `it` to run `func` on it, with respect to backpresure - new 
    items are pulled from `it` only after results are consumed from the result iterator.
    """
    res: ParallelMap[G] = None  # type: ignore[assignment]
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
        yield from res.start()  # type: ignore[misc]
    finally:
        res.stop()
