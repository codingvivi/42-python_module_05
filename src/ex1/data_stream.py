import typing
from abc import ABC, abstractmethod
from typing import Any, NamedTuple


class ProcessorStats(NamedTuple):
    name: str
    processed: int
    remaining: int


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.name: str = ""
        self.types: Any = None
        self.error_message: str = "Invalid input, can't ingest"
        self._buffer: list[tuple[int, str]] = []
        self._processed = 0

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
            raise ValueError(self.error_message)
        items = data if isinstance(data, list) else [data]
        for x in items:
            self._buffer.append((self._processed, str(x)))
            self._processed += 1

    def output(self) -> tuple[int, str]:
        if not self._buffer:
            raise IndexError("No data to output!")
        return self._buffer.pop(0)

    def get_stats(self) -> ProcessorStats:
        return ProcessorStats(self.name, self._processed, len(self._buffer))


class NumericProcessor(DataProcessor):
    def __init__(self, name: str = "Numeric Processor") -> None:
        super().__init__()
        self.name = name
        self.types = (int, float)
        self.error_message = "Improper numeric data"

    def validate(self, data: Any) -> bool:
        return super().validate(data)

    def ingest(self, data: int | float | list[int] | list[float]) -> None:
        super().ingest(data)


class TextProcessor(DataProcessor):
    def __init__(self, name: str = "Text Processor") -> None:
        super().__init__()
        self.name = name
        self.types = str
        self.error_message = "Improper text data"

    def validate(self, data: Any) -> bool:
        return super().validate(data)

    def ingest(self, data: str | list[str]) -> None:
        super().ingest(data)


class LogProcessor(DataProcessor):
    def __init__(self, name: str = "Log Processor") -> None:
        super().__init__()
        self.name = name
        self.types = dict
        self.error_message = "Improper log data"

    def validate(self, data: Any) -> bool:
        return super().validate(data)

    def ingest(self, data: dict[Any, Any] | list[dict[Any, Any]]) -> None:
        super().ingest(data)


class DataStream:
    def __init__(self) -> None:
        self.procs: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.procs.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        for item in stream:
            try:
                for p in self.procs:
                    try:
                        p.ingest(item)
                        # only reached if ingest doesn't raise anything
                        break
                    except ValueError:
                        # if it does continue trying the next member
                        continue
                # only runs if no breaks happened
                # meaning we've looped through all the procs
                else:
                    raise TypeError(
                        "DataStream error - "
                        f"Can't process element in stream: {item}"
                    )
            except Exception as e:
                print(e)
                continue

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self.procs:
            print("No processor found, no data")
            return
        for p in self.procs:
            s = p.get_stats()
            print(
                f"{s.name}: "
                f"total {s.processed} items processed, "
                f"remaining {s.remaining} on processor"
            )


def print_header(title: str) -> None:
    print(f"=== {title} ===")
    print("")


def main() -> None:
    print_header("Code Nexus - Data Stream")

    print("Initialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()
    print()

    print("Registering Numeric Processor")
    num_proc = NumericProcessor()
    stream.register_processor(num_proc)
    print()

    batch = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {
                "log_level": "WARNING",
                "log_message": "Telnet access! Use ssh instead",
            },
            {"log_level": "INFO", "log_message": "User wil is connected"},
        ],
        42,
        ["Hi", "five"],
    ]
    print(f"Send first batch of data on stream: {batch}")
    stream.process_stream(batch)
    stream.print_processors_stats()
    print()

    print("Registering other data processors")
    text_proc = TextProcessor()
    log_proc = LogProcessor()
    stream.register_processor(text_proc)
    stream.register_processor(log_proc)
    print("Send the same batch again")
    stream.process_stream(batch)
    stream.print_processors_stats()
    print()

    print(
        "Consume some elements from the data processors: "
        "Numeric 3, Text 2, Log 1"
    )
    for _ in range(3):
        num_proc.output()
    for _ in range(2):
        text_proc.output()
    for _ in range(1):
        log_proc.output()
    stream.print_processors_stats()


if __name__ == "__main__":
    main()
