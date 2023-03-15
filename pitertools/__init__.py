from concurrent.futures import Executor
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Callable, Iterator, Optional, TypeVar

from pitertools.map_theaded import map_parallel_threaded
from pitertools.map_executor import map_parallel_threadpool


T = TypeVar('T')
G = TypeVar('G')


def map_parallel(
    func: Callable[[T], G],
    it: Iterator[T],
    concurrency: int,
    ordered = False,
    verbose: bool = False,
    executor: Optional[Executor] = None
) -> Iterator[G]:
    """
    Concurrently pulls items from `it` to run `func` on it, with respect to backpresure - new 
    items are pulled from `it` only after results are consumed from the result iterator.
    """
    if executor:
        if isinstance(executor, ThreadPoolExecutor):
            return map_parallel_threadpool(
                executor=executor,
                func=func,
                it=it,
                concurrency=concurrency,
                ordered=ordered,
                verbose=verbose
            )
        else:
            raise NotImplementedError()
    else:
        return map_parallel_threaded(
            func=func,
            it=it,
            num_workers=concurrency,
            ordered=ordered,
            verbose=verbose
        )
