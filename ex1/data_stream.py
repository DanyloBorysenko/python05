from typing import List, Any, Optional, Dict, Union
from abc import ABC, abstractmethod


class DataStream(ABC):
    def __init__(self) -> None:
        super().__init__()
        self.data_type: str = "Generic"
        self.last_batch: List[Any] = []

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria is None:
            return data_batch
        try:
            return [el for el in data_batch if criteria in el]
        except TypeError as e:
            print("Error during filtering data")
            print(e)
            return []

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stream_data: Dict[str, Union[str, int, float]] = {}
        stream_data.update({"type": self.data_type})
        stream_data.update({"el_count": len(self.last_batch)})
        return stream_data


class SensorStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        print("Initializing Sensor Stream...")
        super().__init__()
        self.stream_id = stream_id
        self.data_type = "Environmental Data"

    def process_batch(self, data_batch: List[Any]) -> str:
        sensor_names: List[float] = ["temp", "humidity", "pressure"]
        for el in data_batch:
            if isinstance(el, str) is False:
                return f"Unknown sensor data received: '{el}'"
            if ":" not in el:
                return f"Sensor data does not contain ':': '{el}'"
            name_value: List[str] = el.split(":")
            if len(name_value) != 2:
                return f"Sensor data is not completed '{el}'"
            if name_value[0] not in sensor_names:
                return f"Not existed sensor name '{el}'"
            if name_value[1] == "":
                return f"Value is empty: '{el}'"
        self.last_batch = data_batch
        return f"Processing sensor batch: [{', '.join(data_batch)}]"

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stream_data: Dict[str, Union[str, int, float]] = super().get_stats()
        avg_temp: float
        try:
            temperatures: List[str] = [float(el.split(":")[1])
                                       for el in self.last_batch
                                       if ':' in el and
                                       el.split(":")[0] == "temp"]
            if len(temperatures) == 0:
                avg_temp = 0
            else:
                avg_temp = (sum(temperatures) / len(temperatures))
        except (TypeError, ValueError) as e:
            print("Sensor stream failed to calculate avg temp")
            print(e)
        else:
            stream_data.update({"avg_temp": avg_temp})
        return stream_data


class TransactionStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        print("Initializing Transaction Stream...")
        super().__init__()
        self.stream_id = stream_id
        self.data_type = "Financial Data"

    def process_batch(self, data_batch: List[Any]) -> str:
        operation_names: List[str] = ["buy", "sell"]
        for el in data_batch:
            if isinstance(el, str) is False:
                return f"Unknown operation data received: '{el}'"
            if ":" not in el:
                return f"Operation data does not contain ':': '{el}'"
            name_value: List[str] = el.split(":")
            if len(name_value) != 2:
                return f"Operation data is not completed '{el}'"
            if name_value[0] not in operation_names:
                return f"Not existed operation '{el}'"
            if name_value[1] == "":
                return f"Value is empty: '{el}'"
        self.last_batch = data_batch
        return f"Processing transaction batch: [{', '.join(data_batch)}]"

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stream_data: Dict[str, Union[str, int, float]] = super().get_stats()
        try:
            net_flow: int = 0
            for el in self.last_batch:
                if ":" in el:
                    key_value: List[str] = el.split(":")
                    if key_value[0] == "buy":
                        net_flow += int(key_value[1])
                    elif key_value[0] == "sell":
                        net_flow -= int(key_value[1])
            stream_data.update({"net_flow": str(net_flow if net_flow <= 0
                                                else f"+{net_flow}")})
        except (TypeError, ValueError) as e:
            print("Transaction stream failed to calculate net_flow")
            print(e)
        return stream_data


class EventStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        print("Initializing Event Stream...")
        super().__init__()
        self.stream_id = stream_id
        self.data_type = "System Events"

    def process_batch(self, data_batch: List[Any]) -> str:
        for el in data_batch:
            if isinstance(el, str) is False:
                return f"Unknown event data received: '{el}'"
        self.last_batch = data_batch
        return f"Processing event batch: [{', '.join(data_batch)}]"

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stream_data: Dict[str, Union[str, int, float]] = super().get_stats()
        error_count: int = 0
        for el in self.last_batch:
            if el == "error":
                error_count += 1
        stream_data.update({"error_count": error_count})
        return stream_data


class StreamProcessor():
    def __init__(self) -> None:
        self.batch_number = 1

    def batch_processing(self,
                         mixed_streams: Dict[DataStream, List[Any]]) -> None:
        print(f"Batch {self.batch_number} Results:")
        for stream, batch in mixed_streams.items():
            stream.process_batch(batch)
            stats: Dict[str: Union[str, int, float]] = stream.get_stats()

            print(
                f"- {stats['type']}: "
                f"{stats['el_count']} processed"
            )

    def find_high_temp(self, mixed_streams: List[DataStream]) -> int:
        high_temp: List[str] = []
        for stream in mixed_streams:
            if isinstance(stream, SensorStream):
                temperatures: List[str] = stream.filter_data(stream.last_batch,
                                                             "temp")
                high_temp = [temp for temp in temperatures
                             if float(temp.split(":")[1]) > 30]
        return len(high_temp)


if __name__ == "__main__":
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")
    print()
    batch_res: str
    analysis: Dict[str, Union[str, int, float]]
    stream_id: str
    type: str

    stream_id = "SENSOR_001"
    type = "Environmental Data"
    sensor_input: List[Any] = ["temp:22.5",
                               "humidity:65", "pressure:1013"]
    print(f"Stream ID: {stream_id}, Type: {type}")
    sensor_stream: DataStream = SensorStream(stream_id)
    batch_res = sensor_stream.process_batch(sensor_input)
    print(batch_res)
    analysis = sensor_stream.get_stats()
    print(f"Sensor analysis: {analysis["el_count"]} readings processed, "
          f"avg temp: {analysis["avg_temp"]:.1f}°C")
    print()

    stream_id = "TRANS_001"
    type = "Financial Data"
    transaction_input: List[Any] = ["buy:100",
                                    "sell:150", "buy:75"]
    print(f"Stream ID: {stream_id}, Type: {type}")
    transaction_stream: DataStream = TransactionStream(stream_id)
    batch_res = transaction_stream.process_batch(transaction_input)
    print(batch_res)
    analysis = transaction_stream.get_stats()
    print(f"Transaction analysis: {analysis["el_count"]} operations, "
          f"net flow: {analysis["net_flow"]}")
    print()

    stream_id = "EVENT_001"
    type = "System Events"
    event_input: List[Any] = ["login", "error", "logout"]
    print(f"Stream ID: {stream_id}, Type: {type}")
    event_stream: DataStream = EventStream(stream_id)
    batch_res = event_stream.process_batch(event_input)
    print(batch_res)
    analysis = event_stream.get_stats()
    print(f"Event analysis: {analysis["el_count"]} events, "
          f"{analysis["error_count"]} error detected")
    print()

    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...\n")
    stream_proc: StreamProcessor = StreamProcessor()
    data_batch: Dict[DataStream, List[Any]] = {
            sensor_stream: ["temp:30.7", "temp:20.2", "temp:34"],
            transaction_stream: ["buy:200", "sell:100", "sell:50", "buy:100"],
            event_stream: ["error", "login", "error"]
        }
    stream_proc.batch_processing(data_batch)
    print()
    print("Stream filtering active: High-priority data only")
    print(f"Filtered results: {stream_proc.find_high_temp(data_batch.keys())} "
          f"critical temperatures were found")
