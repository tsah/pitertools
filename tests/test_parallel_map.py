from itertools import repeat
import pytest
import threading
import time

from pitertools import map_parallel

@pytest.mark.parametrize("num_workers", [1, 2, 10, 20])
def test_parmap_correcness(num_workers: int) -> None:
    it = repeat(1, 10)
    func = lambda i: i + 1
    res = [i for i in map_parallel(func, it, num_workers, verbose=True)]
    assert res == list(repeat(2, 10))
    _assert_no_running_threads()


@pytest.mark.parametrize("num_workers", [1, 2, 10, 20])
def test_parmap_parallelism(num_workers: int) -> None:
    it = repeat(1, num_workers)
    def func(i):
        time.sleep(1)
        return threading.get_ident()
    res = {t_id for t_id in map_parallel(func, it, num_workers, verbose=True)}
    assert len(res) == num_workers
    _assert_no_running_threads()


@pytest.mark.parametrize("num_workers", [2, 10, 20])
def test_error_handling(num_workers: int) -> None:
    it = iter(range(num_workers))
    def func(i):
        if i == num_workers - 1:
            raise RuntimeError()
        else:
            return i
    with pytest.raises(RuntimeError):
        res_iter = map_parallel(func, it, num_workers, verbose=True)
        res = []
        for i in res_iter:
            res.append(i)
    assert res == list(range(num_workers-1))
    _assert_no_running_threads()


def _assert_no_running_threads() -> None:
    time.sleep(1)
    assert threading.active_count() == 1

