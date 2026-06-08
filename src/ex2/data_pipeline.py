import typing
from abc import ABC, abstractmethod
from typing import Any, NamedTuple, Protocol


class ProcessorStats(NamedTuple):
    name: str
    processed: int
    remaining: int


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.name: str = ""
        self.types: Any = None
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

    def _format(self, item: Any) -> str:
        return str(item)

    @abstractmethod
    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise ValueError("Invalid input, can't ingest")
        items = data if isinstance(data, list) else [data]
        for x in items:
            self._buffer.append((self._processed, self._format(x)))
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

    def validate(self, data: Any) -> bool:
        return super().validate(data)

    def ingest(self, data: int | float | list[int] | list[float]) -> None:
        super().ingest(data)


class TextProcessor(DataProcessor):
    def __init__(self, name: str = "Text Processor") -> None:
        super().__init__()
        self.name = name
        self.types = str

    def validate(self, data: Any) -> bool:
        return super().validate(data)

    def ingest(self, data: str | list[str]) -> None:
        super().ingest(data)


class LogProcessor(DataProcessor):
    def __init__(self, name: str = "Log Processor") -> None:
        super().__init__()
        self.name = name
        self.types = dict

    def validate(self, data: Any) -> bool:
        return super().validate(data)

    def ingest(self, data: dict[Any, Any]) -> None:
        super().ingest(data)

    def _format(self, item: dict[Any, Any]) -> str:
        return f"{item['log_level']}: {item['log_message']}"


# can use this as the type at callsight,
# as long as any class implmenets process_output the same way
# it'll be valid
class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None: ...


class CSVExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:")
        print(",".join(val for _, val in data))


class JSONExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        body = ", ".join(f'"item_{i}": "{val}"' for i, val in data)
        print("JSON Output:")
        print("{" + body + "}")


class DataStream:
    def __init__(self) -> None:
        self.procs: list[DataProcessor] = []

    def register_processor(
        self, proc: DataProcessor | list[DataProcessor]
    ) -> None:
        new_procs: list[DataProcessor] = (
            proc if isinstance(proc, list) else [proc]
        )
        self.procs.extend(new_procs)

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

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for p in self.procs:
            output_data: list[tuple[int, str]] = []
            requested: int = nb

            while requested and p._buffer:
                output_data.append(p.output())
                requested -= 1

            plugin.process_output(output_data)


def print_header(title: str) -> None:
    print(f"=== {title} ===")
    print("")


def main() -> None:
    print_header("Code Nexus - Data Pipeline")

    print("Initialize Data Stream...\n")
    stream: DataStream = DataStream()
    stream.print_processors_stats()

    print("\nRegistering Processors")
    stream.register_processor(
        [NumericProcessor(), TextProcessor(), LogProcessor()]
    )

    batch1: list[Any] = [
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
    print(f"\nSend first batch of data on stream: {batch1}\n")
    stream.process_stream(batch1)
    stream.print_processors_stats()

    print("\nSend 3 processed data from each processor to a CSV plugin:")
    stream.output_pipeline(3, CSVExportPlugin())
    print()
    stream.print_processors_stats()

    batch2: list[Any] = [
        21,
        ["I love AI", "LLMs are wonderful", "Stay healthy"],
        [
            {"log_level": "ERROR", "log_message": "500 server crash"},
            {
                "log_level": "NOTICE",
                "log_message": "Certificate expires in 10 days",
            },
        ],
        [32, 42, 64, 84, 128, 168],
        "World hello",
    ]
    print(f"\nSend another batch of data: {batch2}\n")
    stream.process_stream(batch2)
    stream.print_processors_stats()

    print("\nSend 5 processed data from each processor to a JSON plugin:")
    stream.output_pipeline(5, JSONExportPlugin())
    print()
    stream.print_processors_stats()


if __name__ == "__main__":
    main()
