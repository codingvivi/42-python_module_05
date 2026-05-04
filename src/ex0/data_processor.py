from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    @abstractmethod
    def validate(self, data: Any) -> bool:
        ...

    @abstractmethod
    def ingest(self, data: Any) -> None:
        ...

    def output(self) -> tuple[int, str]:
        ...


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        ...

    def ingest(self, data):
        ...


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        ...

    def ingest(self, data):
        ...


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        ...

    def ingest(self, data):
        ...


def print_header(title: str) -> None:
    print(f"=== {title} ===")
    print("")


def main() -> None:
    print_header("Code Nexus - Data Processor")


if __name__ == "__main__":
    main()
