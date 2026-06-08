from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.types: Any = None
        self._buffer: list[tuple[int, str]] = []
        self._next_rank = 0

    # must be overriden in children as per contract
    # child becomes abstract if not
    @abstractmethod
    def validate(self, data: Any) -> bool:
        if isinstance(data, self.types):
            return True
        # elif isinstance(data, (list, tuple, set, frozenset)):
        elif isinstance(data, list):
            return all(isinstance(d, self.types) for d in data)
        else:
            return False

    @abstractmethod
    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise ValueError("Invalid input, can't ingest")
        items = data if isinstance(data, list) else [data]
        for x in items:
            self._buffer.append((self._next_rank, str(x)))
            self._next_rank += 1

    def output(self) -> tuple[int, str]:
        if not self._buffer:
            raise IndexError("No data to output!")
        return self._buffer.pop(0)


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        self.types = (int, float)

    def validate(self, data: Any) -> bool:
        return super().validate(data)

    def ingest(self, data: int | float | list[int] | list[float]) -> None:
        super().ingest(data)


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        self.types = str

    def validate(self, data: Any) -> bool:
        return super().validate(data)

    def ingest(self, data: str | list[str]) -> None:
        super().ingest(data)


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        self.types = dict

    def validate(self, data: Any) -> bool:
        return super().validate(data)

    def ingest(self, data: dict[Any, Any] | list[dict[Any, Any]]) -> None:
        super().ingest(data)


def print_header(title: str) -> None:
    print(f"=== {title} ===")
    print("")


def main() -> None:
    print_header("Code Nexus - Data Processor")

    print("Testing Numeric Processor...")

    np = NumericProcessor()
    print(f" Trying to validate input '42': {np.validate(42)}")
    print(f" Trying to validate input 'Hello': {np.validate('Hello')}")

    print(" Test invalid ingestion of string 'foo' without prior validation:")
    try:
        np.ingest("foo")  # type: ignore[arg-type]
    except Exception as e:
        print(f" Got exception: {e}")

    numeric_data: list[int] = [1, 2, 3, 4, 5]
    print(f" Processing data: {numeric_data}")
    np.ingest(numeric_data)
    print(" Extracting 3 values...")
    for _ in range(3):
        rank, value = np.output()
        print(f" Numeric value {rank}: {value}")
    print()

    print("Testing Text Processor...")

    tp = TextProcessor()
    print(f" Trying to validate input '42': {tp.validate(42)}")

    text_data = ["Hello", "Nexus", "World"]
    print(f" Processing data: {text_data}")
    tp.ingest(text_data)
    print(" Extracting 1 value...")
    rank, value = tp.output()
    print(f" Text value {rank}: {value}")
    print()

    print("Testing Log Processor...")
    lp = LogProcessor()
    print(f" Trying to validate input 'Hello': {lp.validate('Hello')}")

    log_data = [
        {"log_level": "NOTICE", "log_message": "Connection to server"},
        {"log_level": "ERROR", "log_message": "Unauthorized access!!"},
    ]
    print(f" Processing data: {log_data}")
    lp.ingest(log_data)
    print(" Extracting 2 values...")
    for _ in range(2):
        rank, value = lp.output()
        print(f" Log entry {rank}: {value}")


if __name__ == "__main__":
    main()
