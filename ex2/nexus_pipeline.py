from typing import List, Dict, Union, Any, Protocol
from abc import ABC, abstractmethod


class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        ...


class InputStage:
    def process(self, data: Any) -> Dict[str, Any]:
        print(f"Input: {data}")
        res: Dict[str, Any] = {}
        if isinstance(data, str) and data.startswith("{"):
            data = data.strip("{}")
            readings: List[str] = data.split(",")
            if len(readings) == 0:
                raise ValueError("InputStage: Empty json data")
            for reading in readings:
                key_value: List[str] = reading.split(":")
                if len(key_value) != 2:
                    raise ValueError(f"InputStage: not correct json format - "
                                     f"{reading}")
                key = key_value[0].strip().strip('"')
                value = key_value[1].strip().strip('"')
                res[key] = value
            res["format"] = "JSON"
            return res
        elif isinstance(data, str) and "," in data:
            lines = [line.strip() for line in data.splitlines() if line.strip()]
            if len(lines) == 0:
                raise ValueError("Input stage: Empty csv data")
            header = lines[0].split(",")
            rows = lines[1:]
            res["format"] = "CSV"
            res["columns"] = header
            res["rows"] = rows
            return res
        elif isinstance(data, list) and all(isinstance(item, float)
                                            for item in data):
            res.update({"values": data})
            res["format"] = "stream"
            return res
        raise ValueError("Input Stage: Unknown data format")


class TransformStage:
    def process(self, data: Any) -> Dict:
        if isinstance(data, dict) is False:
            raise ValueError(f"TransformStage: data is not dict - {data}")
        data: Dict = dict(data)
        if data.get("format", False) is False:
            raise ValueError(("TransformStage: data does not contain 'format' "
                              "key"))
        format = data["format"]
        if format == "JSON":
            if data.get("value", False) is False:
                raise ValueError(("TransformStage: data does not contain "
                                  "'value' key"))
            data["value"] = float(data["value"])
            print("Transform: Enriched with metadata and validation")
        elif format == "CSV":
            if data.get("rows", False) is False:
                raise ValueError(("TransformStage: data does not contain "
                                  "'rows' key"))
            data["rows"] = len(data["rows"])
            print("Transform: Parsed and structured data")
        elif format == "stream":
            if data.get("values", False) is False:
                raise ValueError(("TransformStage: data does not contain "
                                  "'values' key"))
            data["readings"] = len(data["values"])
            data["avg"] = 0 if data["readings"] == 0 else round(
                sum(data["values"]) / data["readings"], 1)
            print(data)
            print("Transform: Aggregated and filtered")
        return data


class OutputStage:
    def process(self, data: Any) -> str:
        try:
            format = data["format"]
            if format == "JSON":
                temp = data["value"]
                range_info = "(Normal range)" if temp < 30 else "(Out of range)"
                print(f"Output: Processed temperature reading: "
                      f"{data["value"]}°C {range_info}")
            elif format == "CSV":
                print(f"Output: User activity logged: "
                      f"{data["rows"]} actions processed")
            elif format == "stream":
                print(f"Output: Stream summary: {data["readings"]} readings, "
                      f"avg: {data["avg"]}°C")
        except Exception:
            raise ValueError(f"OutputStage: data is not correct: {data}")


class ProcessingPipeline(ABC):
    def __init__(self) -> None:
        super().__init__()
        self.stages: List[ProcessingStage] = []

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)

    def run_stages(self, data: Any) -> Union[str, Any]:
        current = data
        for stage in self.stages:
            current = stage.process(current)
        return current

    @abstractmethod
    def process(self, data: Any) -> Any:
        pass


class JSONAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        print(f"Processing data through {self.pipeline_id}...")
        return self.run_stages(data)


class CSVAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        print(f"Processing data through {self.pipeline_id}...")
        return self.run_stages(data)


class StreamAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        print(f"Processing data through {self.pipeline_id}...")
        return self.run_stages(data)


class NexusManager:
    def __init__(self) -> None:
        self.pipelines: List[ProcessingPipeline] = []
        print("Initializing Nexus Manager...")

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        self.pipelines.append(pipeline)

    def process_data(self, data: Any) -> None:
        current = data
        for pipeline in self.pipelines:
            print(f"Processing through {pipeline.__class__.__name__}")
            current = pipeline.process(current)


if __name__ == "__main__":
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===")
    print()

    nex_manager = NexusManager()

    print("Creating Data Processing Pipeline...")

    print("Stage 1: Input validation and parsing")
    input_stage: InputStage = InputStage()

    print("Stage 2: Data transformation and enrichment")
    transform_stage: TransformStage = TransformStage()

    print("Stage 3: Output formatting and delivery")
    output_stage: OutputStage = OutputStage()
    print()
    print("=== Multi-Format Data Processing ===\n")

    json_format = '{"sensor": "temp", "value": 23.5, "unit": "C"}'
    csv_format = "user,action,timestamp\ndanborys, login, 05.02.2026"
    stream_format = [23.5, 26.6, 20.2]

    json_adapter: ProcessingPipeline = JSONAdapter("json_001")
    json_adapter.add_stage(input_stage)
    json_adapter.add_stage(transform_stage)
    json_adapter.add_stage(output_stage)

    print("Processing JSON data through pipeline...")
    json_adapter.process(json_format)
    print()
    print("Processing SCV data through pipeline...")
    json_adapter.process(csv_format)
    print()
    print("Processing stream data through pipeline...")
    json_adapter.process(stream_format)
