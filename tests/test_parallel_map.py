from itertools import repeat
import pytest

from pitertools import map_parallel

@pytest.mark.parametrize("num_workers", [1, 2, 10, 20])
def test_parmap(num_workers: int) -> None:
    it = repeat(1, 10)
    func = lambda i: i + 1
    res = [i for i in map_parallel(func, it, num_workers, verbose=True)]
    assert res == list(repeat(2, 10))
