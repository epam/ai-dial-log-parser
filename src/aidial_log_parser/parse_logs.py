import datetime
import json
import logging
import re
import urllib.parse
from dataclasses import dataclass
from typing import Iterable, Tuple

import click
import fsspec
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.json as pj
from pyarrow import fs

ENVVAR_PREFIX = "DIAL_LOG_PARSER"


DEPLOYMENT_NAME_REGEX = re.compile(
    r"/openai/deployments/([^/]+)(/chat/completions|/embeddings)"
)

DEFAULT_FILENAME_REGEX = (
    r"date=(\d{4}-\d{2}-\d{2})(\d+)-(\w{8}-\w{4}-\w{4}-\w{4}-\w{12}).log(.gz)?"
)

DEFAULT_FILENAME_REGEX_COMPILED = re.compile(DEFAULT_FILENAME_REGEX)

DEPLOYMENT_FIELD_NAME = "deployment"

OUT_PARTITIONING = ds.partitioning(
    pa.schema(
        [
            pa.field(DEPLOYMENT_FIELD_NAME, pa.string()),
            pa.field("date", pa.date32()),
        ]
    )
)


OUT_SCHEMA = pa.schema(
    [
        pa.field(DEPLOYMENT_FIELD_NAME, pa.string()),
        pa.field("date", pa.date32()),
        pa.field("apiType", pa.string(), nullable=True),
        pa.field(
            "chat",
            pa.struct([pa.field("id", pa.string(), nullable=True)]),
            nullable=True,
        ),
        pa.field(
            "project",
            pa.struct([pa.field("id", pa.string(), nullable=True)]),
            nullable=True,
        ),
        pa.field(
            "user",
            pa.struct(
                [
                    pa.field("id", pa.string(), nullable=True),
                    pa.field("title", pa.string(), nullable=True),
                ]
            ),
            nullable=True,
        ),
        pa.field(
            "token_usage",
            pa.struct(
                [
                    pa.field("completion_tokens", pa.int64(), nullable=True),
                    pa.field("prompt_tokens", pa.int64(), nullable=True),
                    pa.field("total_tokens", pa.int64(), nullable=True),
                    pa.field("deployment_price", pa.float64(), nullable=True),
                    pa.field("price", pa.float64(), nullable=True),
                ]
            ),
            nullable=True,
        ),
        pa.field("execution_path", pa.list_(pa.string()), nullable=True),
        pa.field(
            "trace",
            pa.struct(
                [
                    pa.field("trace_id", pa.string(), nullable=True),
                    pa.field("core_span_id", pa.string(), nullable=True),
                    pa.field("core_parent_span_id", pa.string(), nullable=True),
                ]
            ),
            nullable=True,
        ),
        pa.field(
            "request",
            pa.struct(
                [
                    pa.field("protocol", pa.string(), nullable=True),
                    pa.field("method", pa.string(), nullable=True),
                    pa.field("uri", pa.string(), nullable=True),
                    pa.field("time", pa.string(), nullable=True),
                    pa.field("body", pa.string(), nullable=True),
                ]
            ),
            nullable=True,
        ),
        pa.field(
            "response",
            pa.struct(
                [
                    pa.field("status", pa.string(), nullable=True),
                    pa.field("upstream_uri", pa.string(), nullable=True),
                    pa.field("body", pa.string(), nullable=True),
                ]
            ),
            nullable=True,
        ),
        pa.field(
            "assembled_response",
            pa.string(),
            nullable=True,
        ),
        pa.field("question", pa.string(), nullable=True),
        pa.field("answer", pa.string(), nullable=True),
    ]
)


logging.basicConfig(level=logging.INFO)


def isDebugEnabled():
    return logging.getLogger().isEnabledFor(logging.DEBUG)


@dataclass
class InputFile:
    date: pa.Date32Scalar
    path: str

    @staticmethod
    def from_file_path(
        file_path: str, filename_regex: re.Pattern = DEFAULT_FILENAME_REGEX_COMPILED
    ) -> "InputFile | None":
        match = filename_regex.match(file_path.split("/")[-1])
        if not match:
            logging.warning(f"Skipping file {file_path} with invalid name")
            return None
        date_str = match.group(1)
        return InputFile(
            date=pa.scalar(datetime.date.fromisoformat(date_str), type=pa.date32()),
            path=file_path,
        )


def read_data(input_files: list[InputFile], filesystem):
    read_options = pj.ReadOptions(
        block_size=64 << 20
    )  # 64MiB, single field should not exceed 2 blocks
    with click.progressbar(input_files, label="Reading files", show_pos=True) as files:
        for input_file in files:
            logging.info(f"Reading file {input_file.path}")
            with filesystem.open_input_stream(input_file.path) as f:
                table = pj.read_json(f, read_options)
                for batch in table.to_batches():
                    yield input_file.date, batch


def list_files(
    input_dir: str,
    filesystem,
    log_date: pa.Date32Scalar,
    filename_regex: re.Pattern,
) -> list[InputFile]:
    files = filesystem.get_file_info(fs.FileSelector(input_dir, recursive=True))
    input_files = [
        InputFile.from_file_path(file_info.path, filename_regex)
        for file_info in files
        if file_info.is_file
    ]
    return [f for f in input_files if f and f.date == log_date]


def parse_deployment_name(uri: str) -> str | None:
    try:
        path = urllib.parse.urlparse(uri).path
        match = DEPLOYMENT_NAME_REGEX.match(path)
        return match.group(1) if match else None
    except Exception as e:
        logging.error(f"Failed to parse deployment name from uri {uri}: {e}")
        return None


def add_deployment_name_from_uri(batch: pa.RecordBatch) -> pa.RecordBatch:
    deployment_name = [
        parse_deployment_name(r["uri"].as_py()) for r in batch["request"]
    ]
    new_batch = batch.add_column(0, DEPLOYMENT_FIELD_NAME, deployment_name)
    return new_batch


def filter_invalid_field(
    batch: pa.RecordBatch, traces: pa.Array, field_name: str
) -> pa.RecordBatch:
    filter_mask: pa.BooleanArray = batch[field_name].is_valid()
    if pc.all(filter_mask).as_py():  # type: ignore [reportAttributeAccessIssue]
        return batch

    batch_filtered = batch.filter(filter_mask)

    logging.warning(
        f"Removed {batch.num_rows - batch_filtered.num_rows} rows with invalid {field_name}"
    )
    if isDebugEnabled():
        for trace in traces.filter(pc.invert(filter_mask)):  # type: ignore [reportAttributeAccessIssue]
            logging.debug(f"Filtered out empty {field_name} with trace={trace}")
    return batch_filtered


def fill_missing_nested(column: pa.Array, target_type: pa.DataType) -> pa.StructArray:
    if column.type.equals(target_type):
        return column

    if pa.types.is_struct(target_type) != pa.types.is_struct(column.type):
        raise ValueError(f"Mismatched types: {column.type} and {target_type}")

    if not pa.types.is_struct(target_type):
        return column.cast(target_type)

    new_columns = []
    for field in target_type:
        index = column.type.get_field_index(field.name)
        new_columns.append(
            pa.nulls(len(column), field.type)
            if index == -1
            else fill_missing_nested(column.field(index), field.type)
        )

    return pa.StructArray.from_arrays(
        new_columns, fields=[field for field in target_type]
    )


def fill_missing(batch: pa.RecordBatch, schema: pa.Schema) -> pa.RecordBatch:
    for field in schema:
        index = batch.schema.get_field_index(field.name)
        if index == -1:
            batch = batch.append_column(
                field.name,
                pa.nulls(batch.num_rows, field.type),
            )
        else:
            column = batch.column(index)
            if not field.type.equals(column.type):
                batch = batch.set_column(
                    index, field, fill_missing_nested(column, field.type)
                )

    return batch


def get_traces_column(batch: pa.RecordBatch) -> pa.Array:
    if "trace" in batch.column_names:
        return batch["trace"]
    return pa.array([None] * batch.num_rows)


def extract_question(request_body: str, trace: pa.StructScalar | None) -> str | None:
    try:
        request_json = json.loads(request_body)
    except json.JSONDecodeError:
        logging.debug("Failed to decode JSON for request.body: %s", request_body)
        # We do not want to disclose the request body in logs in non-debug mode
        logging.warning(
            f"Failed to decode request.body JSON for line with trace={trace}"
        )
        return None
    messages = request_json.get("messages", [])
    if not messages:
        return None
    last_message = messages[-1]
    if not last_message.get("role") == "user":
        return None
    # gpt-4-vision adapter does not follow our API format and may have list here
    content = last_message.get("content")
    if isinstance(content, str):
        return content
    return None


def extract_questions(batch: pa.RecordBatch, traces: pa.Array) -> pa.RecordBatch:
    requests = batch["request"]
    questions = [
        extract_question(r["body"].as_py(), t)
        for r, t in zip(requests, traces, strict=True)
    ]
    return batch.append_column("question", questions)


def extract_answer(
    assembled_response: str, trace: pa.StructScalar | None
) -> str | None:
    try:
        response_json = json.loads(assembled_response)
    except json.JSONDecodeError:
        logging.debug(
            "Failed to decode JSON for assembled_response: %s", assembled_response
        )
        # We do not want to disclose the response content in logs in non-debug mode
        logging.warning(
            f"Failed to decode assembled_response JSON for line with trace={trace}"
        )
        return None
    try:
        choices = response_json.get("choices", [])
    except AttributeError:
        logging.warning(
            f"Root of the assembled_response JSON is not an object for line with trace={trace}"
        )
        return None
    if not choices:
        return None
    return choices[0].get("message", {}).get("content")


def extract_answers(batch: pa.RecordBatch, traces: pa.Array) -> pa.RecordBatch:
    if "assembled_response" not in batch.column_names:
        return batch
    assembled_responses = batch["assembled_response"]
    answers = [
        extract_answer(r.as_py(), t)
        for r, t in zip(assembled_responses, traces, strict=True)
    ]
    return batch.append_column("answer", answers)


def check_input_batch(batch: pa.RecordBatch) -> bool:
    for name, column in zip(batch.column_names, batch.columns, strict=True):
        try:
            column.validate(full=True)
        except pa.ArrowInvalid as e:
            logging.error(f"Skipping invalid batch with {batch.num_rows} rows.")
            logging.error(f"Error in column {name!r}: {e}")
            return False
    return True


def process_batches(
    batches: Iterable[Tuple[str, pa.RecordBatch]], schema: pa.Schema = OUT_SCHEMA
) -> Iterable[pa.RecordBatch]:
    for date, batch in batches:
        if not check_input_batch(batch):
            continue

        if DEPLOYMENT_FIELD_NAME not in batch.column_names:
            batch = add_deployment_name_from_uri(batch)

        traces = get_traces_column(batch)

        batch = filter_invalid_field(batch, traces, DEPLOYMENT_FIELD_NAME)

        # Some rows may be removed by the filter, so we need to get new traces column
        traces = get_traces_column(batch)
        batch = batch.append_column("date", [date] * batch.num_rows)
        batch = extract_questions(batch, traces)
        batch = extract_answers(batch, traces)

        batch = fill_missing(batch, schema)
        batch.validate(full=True)
        yield batch


def file_visitor(written_file):
    logging.info(
        f"Written file {written_file.path} with size {written_file.size}"
        + f" bytes, {written_file.metadata.num_rows} rows"
    )


def parse_logs(
    input_dir: str,
    output_dir: str,
    date: pa.Date32Scalar,
    filename_regex: re.Pattern = DEFAULT_FILENAME_REGEX_COMPILED,
):
    in_fs_fsspec, input_dir_path = fsspec.url_to_fs(input_dir)
    in_fs = fs.PyFileSystem(fs.FSSpecHandler(in_fs_fsspec))
    if not in_fs.get_file_info(fs.FileSelector(input_dir_path)):
        raise FileNotFoundError(f"Input directory {input_dir} does not exist")

    input_files = list_files(input_dir_path, in_fs, date, filename_regex)
    logging.info(f"Found {len(input_files)} files for date {date}")
    logging.debug(f"Input files: {input_files}")

    out_fs_fsspec, output_dir_path = fsspec.url_to_fs(output_dir)
    out_fs = fs.PyFileSystem(fs.FSSpecHandler(out_fs_fsspec))
    if not out_fs.get_file_info(output_dir_path):
        raise FileNotFoundError(f"Output directory {output_dir} does not exist")

    input_batches_iter = read_data(input_files, filesystem=in_fs)
    logging.debug(f"Output schema: {OUT_SCHEMA}")

    output_batches_iter = process_batches(input_batches_iter, OUT_SCHEMA)
    ds.write_dataset(
        output_batches_iter,
        output_dir_path,
        filesystem=out_fs,
        format="parquet",
        partitioning=OUT_PARTITIONING,
        schema=OUT_SCHEMA,
        existing_data_behavior="delete_matching",
        use_threads=False,
        file_visitor=file_visitor,
    )


@click.command()
@click.option(
    "-i",
    "--input",
    type=str,
    required=True,
    help="Path to input log directory",
    show_envvar=True,
)
@click.option(
    "-o",
    "--output",
    type=str,
    required=True,
    help="Path to output log directory",
    show_envvar=True,
)
@click.option(
    "-d",
    "--date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(datetime.date.today() - datetime.timedelta(days=1)),
    help="Date to process logs for",
    show_default=True,
    show_envvar=True,
)
@click.option(
    "--debug",
    is_flag=True,
    help="Enable debug logging",
    default=False,
    show_default=True,
    show_envvar=True,
)
@click.option(
    "--filename-regex",
    type=str,
    help="Regex to match log file names",
    default=DEFAULT_FILENAME_REGEX,
    show_default=True,
    show_envvar=True,
)
def main(input, output, date, debug, filename_regex):
    """Parse dial log files and repack it to parquet dataset."""
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)

    logging.info(f"Input dir: {input}")
    logging.info(f"Output dir: {output}")
    logging.info(f"Date: {date}")
    filename_regex_compiled = re.compile(filename_regex)
    parse_logs(
        input, output, pa.scalar(date, type=pa.date32()), filename_regex_compiled
    )


if __name__ == "__main__":
    main(auto_envvar_prefix=ENVVAR_PREFIX)
