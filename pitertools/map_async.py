import asyncio
from collections.abc import Iterator, AsyncIterator
from typing import Callable, TypeVar, Awaitable, Union

T = TypeVar('T')
G = TypeVar('G')


async def map_parallel_async(
    func: Callable[[T], Awaitable[G]],
    it: Union[Iterator[T], AsyncIterator[T]],
    concurrency: int,
) -> AsyncIterator[G]:
    try:
        async def get_next() -> T:
            try:
                if isinstance(it, AsyncIterator) or hasattr(it, '__anext__'):
                    return await it.__anext__()  # type: ignore[assignment]
                else:
                    return next(it)
            except StopIteration:
                raise StopAsyncIteration

        running_tasks: list[asyncio.Task[G]] = []
        while True:
            while len(running_tasks) < concurrency:
                current_item = await get_next()
                running_tasks.append(asyncio.create_task(func(current_item)))  # type: ignore[arg-type]
            done, pending = await asyncio.wait(running_tasks, return_when=asyncio.FIRST_COMPLETED)
            running_tasks = list(pending)
            for item in done:
                yield item.result()
    except StopAsyncIteration:
        pass
