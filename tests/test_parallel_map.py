from concurrent.futures.thread import ThreadPoolExecutor
from contextlib import contextmanager
from itertools import repeat
from typing import Iterator, Optional
import pytest
import threading
import time

from pitertools import map_parallel


@contextmanager
def _setup_executor(
    use_executor: bool,
    executor_num_workers: int
) -> Iterator[Optional[ThreadPoolExecutor]]:
    executor = ThreadPoolExecutor(max_workers=executor_num_workers) if use_executor else None
    try:
        yield executor
    finally:
        if executor:
            executor.shutdown()

@pytest.fixture(autouse=True)
def assert_no_running_threads() -> Iterator[None]:
    yield
    time.sleep(1)
    assert threading.active_count() == 1

def test_test() -> None:
    with _setup_executor(True, 1) as executor:
        it = repeat(1, 10)
        func = lambda i: i + 1
        res = [i for i in map_parallel(func, it, 1, executor=executor, verbose=True)]
        assert res == list(repeat(2, 10))

@pytest.mark.parametrize("concurrency", [1, 2, 10, 20])
@pytest.mark.parametrize("use_executor", [True, False])
@pytest.mark.parametrize("executor_num_workers", [1, 2, 10, 20])
def test_parmap_correcness(
    concurrency: int,
    use_executor: bool,
    executor_num_workers: int
) -> None:
    with _setup_executor(use_executor, executor_num_workers) as executor:
        it = repeat(1, 10)
        func = lambda i: i + 1
        res = [i for i in map_parallel(func, it, concurrency, executor=executor, verbose=True)]
        assert res == list(repeat(2, 10))


@pytest.mark.parametrize("concurrency", [1, 2, 10, 20])
@pytest.mark.parametrize("use_executor", [True, False])
@pytest.mark.parametrize("executor_num_workers", [1, 2, 10, 20])
def test_parmap_parallelism(
    concurrency: int,
    use_executor: bool,
    executor_num_workers: int
) -> None:
    with _setup_executor(use_executor, executor_num_workers) as executor:
        it = repeat(1, concurrency)
        def func(i):
            time.sleep(1)
            return threading.get_ident()
        res = {
            t_id
            for t_id
            in map_parallel(func, it, concurrency, executor=executor, verbose=True)
        }
        assert len(res) == concurrency


@pytest.mark.parametrize("concurrency", [1, 2, 10, 20])
@pytest.mark.parametrize("use_executor", [True, False])
@pytest.mark.parametrize("executor_num_workers", [1, 2, 10, 20])
def test_error_handling(concurrency: int, use_executor: bool, executor_num_workers: int) -> None:
    with _setup_executor(use_executor, executor_num_workers) as executor:
        it = iter(range(concurrency))
        def func(i):
            if i == concurrency - 1:
                raise RuntimeError()
            else:
                return i
        with pytest.raises(RuntimeError):
            res_iter = map_parallel(func, it, concurrency, executor=executor, verbose=True)
            res = []
            for i in res_iter:
                res.append(i)
        assert res == list(range(concurrency-1))


@pytest.mark.parametrize("concurrency", [2, 10, 20])
@pytest.mark.parametrize("use_executor", [True, False])
@pytest.mark.parametrize("executor_num_workers", [2, 10, 20])
def test_ordered_unordered(
    concurrency: int,
    use_executor: bool,
    executor_num_workers: int
) -> None:
    with _setup_executor(use_executor, executor_num_workers) as executor:
        n = concurrency + 10
        def it() -> Iterator[int]:
            return iter(range(n))

        def func(i):
            time.sleep(0.1 *(n - i))  # Smaller i <> more sleep
            return i

        expected_sorted = list(range(n))
        res = list(map_parallel(func, it(), concurrency, executor=executor, verbose=True))
        assert res != expected_sorted  # sleep pattern makes it very unlikely to be sorted

        res = list(map_parallel(
            func,
            it(),
            concurrency,
            executor=executor,
            ordered=True,
            verbose=True
        ))
        assert res == expected_sorted


def test_threadpool_map_doesnt_cause_starvation() -> None:
    class Tester:
        def __init__(self) -> None:
            self.did_run = False

        def run(self) -> None:
            self.did_run = True

    with ThreadPoolExecutor(max_workers=1) as executor:
        func = lambda i: i
        it = iter(range(1000))
        res_generator = map_parallel(func, it, concurrency=100)
        tester = Tester()
        executor.submit(tester.run)
        time.sleep(1)
        assert tester.did_run
        list(res_generator)  # flush all threads
