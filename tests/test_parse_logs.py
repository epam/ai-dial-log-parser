import datetime
import json
import logging

import pyarrow as pa
import pyarrow.dataset as ds
from fsspec.implementations.memory import MemoryFileSystem

from aidial_log_parser.parse_logs import InputFile, parse_deployment_name, parse_logs

logging.getLogger().setLevel(logging.DEBUG)


def test_input_file():
    input_file = InputFile.from_file_path(
        "s3://my-s3-bucket/date=2023-11-061699285645-11111111-2222-3333-4444-555555555555.log.gz"
    )
    assert input_file is not None
    assert input_file.date == pa.scalar(datetime.date(2023, 11, 6), type=pa.date32())


def test_parse_deployment_name():
    assert (
        parse_deployment_name(
            "/openai/deployments/anthropic.claude/chat/completions?api-version=2024-02-01"
        )
        == "anthropic.claude"
    )

    assert (
        parse_deployment_name("/openai/deployments/anthropic.claude/unknown/url")
        is None
    )


def test_parse_logs():
    fs = MemoryFileSystem()
    fs.mkdir("/logs1")
    fs.mkdir("/parsed_logs1")

    with fs.open(
        "/logs1/date=2023-11-061699285645-11111111-2222-3333-4444-555555555555.log", "w"
    ) as f:
        data = {
            "request": {
                "protocol": "HTTP/1.1",
                "method": "POST",
                "uri": "/openai/deployments/my_llm_app/chat/completions?api-version=2024-02-01",
                "time": "2023-11-06T01:23:45.678",
                "body": json.dumps(
                    {
                        "messages": [
                            {
                                "role": "user",
                                "content": "Please sequentially count from 1 to 9999.",
                            }
                        ],
                        "temperature": 1,
                        "stream": True,
                        "model": "my_llm_app",
                    }
                ),
            },
            "response": {
                "status": 200,
                "time": "2023-11-06T01:23:45.678",
                "body": "some chunked data...",
            },
            "assembled_response": json.dumps(
                {
                    "choices": [
                        {
                            "message": {
                                "role": "assistant",
                                "content": "No, I cannot do that.",
                            }
                        }
                    ]
                }
            ),
        }
        json.dump(data, f)

    date = pa.scalar(datetime.date(2023, 11, 6), type=pa.date32())
    parse_logs("memory://logs1/", "memory://parsed_logs1/", date=date)

    result = ds.dataset(
        "/parsed_logs1/",
        format="parquet",
        partitioning=ds.partitioning(
            schema=pa.schema(
                [
                    pa.field("deployment", pa.string()),
                    pa.field("date", pa.date32()),
                ]
            )
        ),
        filesystem=fs,
    ).to_table()

    assert result.column("deployment").to_pylist() == ["my_llm_app"]
    assert result.column("date").to_pylist() == [datetime.date(2023, 11, 6)]
    assert result.column("assembled_response").to_pylist() == [
        '{"choices": [{"message": {"role": "assistant", "content": "No, I cannot do that."}}]}'
    ]
    assert result.column("question").to_pylist() == [
        "Please sequentially count from 1 to 9999."
    ]
    assert result.column("answer").to_pylist() == ["No, I cannot do that."]


def test_parse_logs_with_incorrect_assembled_response():
    fs = MemoryFileSystem()
    fs.mkdir("/logs2")
    fs.mkdir("/parsed_logs2")

    with fs.open(
        "/logs2/date=2023-11-061699285645-11111111-2222-3333-4444-555555555555.log", "w"
    ) as f:
        data = {
            "request": {
                "protocol": "HTTP/1.1",
                "method": "POST",
                "uri": "/openai/deployments/my_llm_app/chat/completions?api-version=2024-02-01",
                "time": "2023-11-06T01:23:45.678",
                "body": json.dumps(
                    {
                        "messages": [
                            {
                                "role": "user",
                                "content": "Please sequentially count from 1 to 9999.",
                            }
                        ],
                        "temperature": 1,
                        "stream": True,
                        "model": "my_llm_app",
                    }
                ),
            },
            "response": {
                "status": 500,
                "time": "2023-11-06T01:23:45.678",
                "body": "Error: something is bad!",
            },
            # Invalid JSON in assembled_response
            "assembled_response": "Error: something is bad!",
        }
        json.dump(data, f)

    date = pa.scalar(datetime.date(2023, 11, 6), type=pa.date32())
    parse_logs("memory://logs2/", "memory://parsed_logs2/", date=date)

    result = ds.dataset(
        "/parsed_logs2/",
        format="parquet",
        partitioning=ds.partitioning(
            schema=pa.schema(
                [
                    pa.field("deployment", pa.string()),
                    pa.field("date", pa.date32()),
                ]
            )
        ),
        filesystem=fs,
    ).to_table()

    assert result.column("deployment").to_pylist() == ["my_llm_app"]
    assert result.column("date").to_pylist() == [datetime.date(2023, 11, 6)]
    assert result.column("assembled_response").to_pylist() == [
        "Error: something is bad!"
    ]
    assert result.column("question").to_pylist() == [
        "Please sequentially count from 1 to 9999."
    ]
    assert result.column("answer").to_pylist() == [None]
