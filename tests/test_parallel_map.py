from concurrent.futures.thread import ThreadPoolExecutor
from contextlib import contextmanager
from itertools import repeat
from typing import Iterator, Optional
from pitertools.map_executor import ExecutorShutdownError
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
            executor.shutdown(wait=True)
        time.sleep(10)

@pytest.fixture(autouse=True)
def assert_no_running_threads() -> Iterator[None]:
    yield
    time.sleep(1)
    assert threading.active_count() == 1

def test_test() -> None:
    with _setup_executor(True, 1) as executor:
        it = repeat(1, 10)
        def func(i):
            return i + 1
        with map_parallel(func, it, 1, executor=executor, verbose=True) as par_it:
            res = [i for i in par_it]
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
        def func(i):
            return i + 1
        with map_parallel(func, it, concurrency, executor=executor, verbose=True) as par_it:
            res = [i for i in par_it]
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

        with map_parallel(func, it, concurrency, executor=executor, verbose=True) as par_it:
            res = {
                t_id
                for t_id
                in par_it
            }
            if use_executor:
                assert len(res) == (
                    concurrency
                    if concurrency < executor_num_workers
                    else executor_num_workers
                )
            else:
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
            with map_parallel(func, it, concurrency, executor=executor, verbose=True) as res_iter:
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
        with map_parallel(func, it(), concurrency, executor=executor, verbose=True) as par_it:
            res = list(par_it)
            assert res != expected_sorted  # sleep pattern makes it very unlikely to be sorted

        with map_parallel(
            func,
            it(),
            concurrency,
            executor=executor,
            ordered=True,
            verbose=True
        ) as par_it:
            res = list(par_it)
            assert res == expected_sorted


def test_threadpool_map_doesnt_cause_starvation() -> None:
    class Tester:
        def __init__(self) -> None:
            self.did_run = False

        def run(self) -> None:
            self.did_run = True

    with ThreadPoolExecutor(max_workers=1) as executor:
        def func(i):
            return i
        it = iter(range(1000))
        with map_parallel(func, it, concurrency=100) as res_generator:
            tester = Tester()
            executor.submit(tester.run)
            time.sleep(1)
            assert tester.did_run
            list(res_generator)  # flush all threads


def test_threadpool_is_shutdown_during_processing() -> None:
    def func(i):
        return i
    it = iter(range(10))
    executor = ThreadPoolExecutor(max_workers=1)
    with map_parallel(func, it, concurrency=1, executor=executor, verbose=True) as res_generator:
        executor.shutdown()
        with pytest.raises(ExecutorShutdownError):
            list(res_generator)


@pytest.mark.parametrize("use_executor", [True, False])
def test_closing_context_manager(
    use_executor: bool,
) -> None:
    with _setup_executor(use_executor, 10) as executor:
        def func(i):
            return i
        it = iter(range(100))
        with map_parallel(func, it, concurrency=10, executor=executor, verbose=True) as par_it:
            next(par_it)
            next(par_it)
            print("Done with map")
            # iterator is not drained at this point, if closing is not correct the test will
            # fail on hanging threads
        print("Done with executor")
