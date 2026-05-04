from typing import Protocol


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class CSVExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class JSONExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class DataStream:
    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        ...


def print_header(title: str) -> None:
    print(f"=== {title} ===")
    print("")


def main() -> None:
    print_header("Code Nexus - Data Pipeline")


if __name__ == "__main__":
    main()
