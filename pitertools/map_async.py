import asyncio
from collections.abc import Coroutine, Iterator, AsyncIterator
from concurrent.futures.thread import ThreadPoolExecutor
import random
from typing import Callable, TypeVar, Awaitable

T = TypeVar('T')
G = TypeVar('G')


async def map_parallel_async(
    func: Callable[[T], Awaitable[G]],
    it: Iterator[T] | AsyncIterator[T],
    concurrency: int,
) -> AsyncIterator[G]:
    try:
        async def get_next() -> T:
            try:
                if isinstance(it, AsyncIterator) or hasattr(it, '__anext__'):
                    return await anext(it)
                else:
                    return next(it)
            except StopIteration:
                raise StopAsyncIteration

        running_tasks: list[Coroutine[None, None, T]] = []
        while True:
            while len(running_tasks) < concurrency:
                current_item = await get_next()
                running_tasks.append(asyncio.create_task(func(current_item)))
            done, pending = await asyncio.wait(running_tasks, return_when=asyncio.FIRST_COMPLETED)
            running_tasks = list(pending)
            for item in done:
                yield item.result()
    except StopAsyncIteration:
        pass

async def main():
    input = iter(range(100))
    async def processor(i):
        await asyncio.sleep(random.random())
        return i

    # async for i in map_parallel_async(processor, input, concurrency=10):
    #     print(i)
    
    async def generator():
        for i in range(100):
            yield i
    async for i in map_parallel_async(processor, generator(), concurrency=10):
        print(i)


if __name__ == "__main__":
    asyncio.run(main())
