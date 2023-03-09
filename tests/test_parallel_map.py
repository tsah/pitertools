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


@pytest.mark.parametrize("num_workers", [1, 2, 10, 20])
def test_parmap_parallelism(num_workers: int) -> None:
    it = repeat(1, num_workers)
    def func(i):
        time.sleep(1)
        return threading.get_ident()
    res = {t_id for t_id in map_parallel(func, it, num_workers, verbose=True)}
    assert len(res) == num_workers
