from abc import ABC, abstractmethod
from typing import Any, List


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return "Output: " + result


class NumericProcessor(DataProcessor):
    def __init__(self):
        super().__init__()
        print("Initializing Numeric Processor...")

    def process(self, data: Any) -> str:
        if self.validate(data) is True:
            total_sum = sum(data)
            total_len = len(data)
            return (f"Processed {total_len} numeric values, sum={total_sum}, "
                    f"avg={(total_sum / total_len):.1f}")
        raise ValueError("Not correct data for NumericProcessor")

    def validate(self, data: Any) -> bool:
        try:
            for num in data:
                num + 1
            return True
        except TypeError:
            return False


class TextProcessor(DataProcessor):
    def __init__(self):
        super().__init__()
        print("Initializing Text Processor...")

    def process(self, data: Any) -> str:
        if self.validate(data) is True:
            words: List[str] = data.split()
            return (f"Processed text: {len(data)} characters, "
                    f"{len(words)} words")
        raise ValueError("Not correct data for TextProcessor")

    def validate(self, data: Any) -> bool:
        try:
            data + ""
            return data.find("ERROR:") == -1 and data.find("INFO:") == -1
        except TypeError:
            return False


class LogProcessor(DataProcessor):
    def __init__(self):
        super().__init__()
        print("Initializing Log Processor...")

    def process(self, data: Any) -> str:
        if self.validate(data) is True:
            str_data = str(data)
            index: int = str_data.find(":")
            level: str = str_data[:index]
            message: str = ""
            if level == "ERROR":
                message = "[ALERT]"
            elif level == "INFO":
                message = "[INFO]"
            return (f"{message} {str_data[:index]} level detected:"
                    f"{str_data[index + 1:]}")
        raise ValueError("Not correct data for LogProcessor")

    def validate(self, data: Any) -> bool:
        try:
            data + ""
            data_len: int = len(data)
            for index in range(0, data_len):
                if data[index] == ":":
                    if index != 0 and index != data_len - 1:
                        return True
            return False
        except TypeError:
            return False

    # def format_output(self, result: str) -> str:
    #     return "Special " + super().format_output(result)


if __name__ == "__main__":
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")
    print()

    data: Any = [1, 2, 3, 4, 5]
    num_proc: DataProcessor = NumericProcessor()
    print(f"Processing data: {data}")
    if num_proc.validate(data) is True:
        print("Validation: Numeric data verified")
    print(f"{num_proc.format_output(num_proc.process(data))}")

    print()

    data = "Hello Nexus World"
    text_proc: DataProcessor = TextProcessor()
    print(f"Processing data: \"{data}\"")
    if text_proc.validate(data) is True:
        print("Validation: Text data verified")
    print(f"{text_proc.format_output(text_proc.process(data))}")

    print()

    data = "ERROR: Connection timeout"
    log_proc: DataProcessor = LogProcessor()
    print(f"Processing data: \"{data}\"")
    if log_proc.validate(data) is True:
        print("Validation: Log entry verified")
    print(f"{log_proc.format_output(log_proc.process(data))}")

    print()

    print("=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")
    data_types: List = [[2, 2, 2], "Example text", "INFO: System ready"]
    processors: List[DataProcessor] = [num_proc, text_proc, log_proc]
    for ind in range(0, len(processors)):
        try:
            print(f"Result {ind + 1}: "
                  f"{processors[ind].process(data_types[ind])}")
        except ValueError as e:
            print(e)
    # for data_type in data_types:
    #     try:
    #         print(f"Result 1: {processors[0].process(data_type)}")
    #     except ValueError:
    #         continue
    # for data_type in data_types:
    #     try:
    #         print(f"Result 2: {processors[1].process(data_type)}")
    #     except ValueError:
    #         continue
    # for data_type in data_types:
    #     try:
    #         print(f"Result 3: {processors[2].process(data_type)}")
    #     except ValueError:
    #         continue
    print("\nFoundation systems online. Nexus ready for advanced streams.")
