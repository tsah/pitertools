from typing import Generic, Iterator, Protocol, TypeVar

T = TypeVar('T', covariant=True)

class ParallelMap(Protocol, Generic[T]):
    def start(self) -> Iterator[T]:
        raise NotImplementedError

    def stop(self) -> None:
        raise NotImplementedError
