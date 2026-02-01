from typing import List, Any, Optional, Dict, Union
from abc import ABC, abstractmethod


class DataStream(ABC):
    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        pass


class SensorStream(DataStream):
    def __init__(self, stream_id: str):
        super().__init__()
        self.stream_id = stream_id
        print("Initializing Sensor Stream...")

    def process_batch(self, data_batch: List[Any]) -> str:
        str_list: List[str] = [f"{key}:{value}" for pair in data_batch for key, value in pair.items()]
        return str(str_list)


class TransactionStream(DataStream):
    def __init__(self, stream_id: str):
        super().__init__()
        self.stream_id = stream_id
        print("Initializing Transaction Stream...")

    def process_batch(self, data_batch: List[Any]) -> str:
        raise NotImplementedError


class EventStream(DataStream):
    def __init__(self, stream_id: str):
        super().__init__()
        self.stream_id = stream_id
        print("Initializing Event Stream...")

    def process_batch(self, data_batch: List[Any]) -> str:
        raise NotImplementedError


class StreamProcessor():
    def batch_processing(self, streams: List[DataStream]) -> None:
        pass


if __name__ == "__main__":
    sensor_stream: DataStream = SensorStream("SENSOR_001")
    result = sensor_stream.process_batch([{"temp": 22.5}, {"humidity": 65}, {"pressure": 1013}])
    print(result)
    transaction_stream: DataStream = TransactionStream("TRANS_001")
    event_stream: DataStream = EventStream("EVENT_001")

    stream_proc: StreamProcessor = StreamProcessor()
    stream_proc.batch_processing([sensor_stream,
                                  transaction_stream, event_stream])
