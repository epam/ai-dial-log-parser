# Dial log parser

## Overview

Dial log parser is a tool to parse dial log files and repack it to parquet dataset.

Example:
```
docker run ai-dial-log-parser:development --input s3://bucket-with-dial-core-logs/ --output s3://bucket-with-parsed-logs/parsed_logs
```
The command above will read files like `s3://bucket-with-dial-core-logs/date=2023-11-061699285645-11111111-2222-3333-4444-555555555555.log.gz` for yesterday's date, split the logs by deployment name and repack it into parquet tables.

Example of list of output parquet files:
```
s3://bucket-with-dial-core-logs/parsed_logs/some-assistant/2023-11-06/part-0.parquet
...
s3://bucket-with-dial-core-logs/parsed_logs/gpt-35-turbo/2023-11-06/part-0.parquet
s3://bucket-with-dial-core-logs/parsed_logs/gpt-35-turbo/2023-11-06/part-1.parquet
...
s3://bucket-with-dial-core-logs/parsed_logs/some-application/2023-11-06/part-0.parquet
```
Then you could configure an access control by prefixes like `s3://bucket-with-dial-core-logs/parsed_logs/some-application/`, to allow the developers of the application to have an access to their prompt logs.

The following directory structure could be read by the tools like pyarrow as a single dataset.
```python
import pyarrow.dataset as ds

data = ds.dataset(
    "s3://bucket-with-dial-core-logs/parsed_logs/",
    partitioning=ds.partitioning(field_names=["deployment_name", "date"]),
    exclude_invalid_files=True)
data.head(
    10,
    filter=ds.field("deployment_name") == "some-application"
).to_pandas()
```

## Configuration

The configuration could be set using environment variables or as command-line arguments.

### Environment variables

Following environment variables could be used for the configuration:

|Variable|Required|Description|
|---|---|---|
|`DIAL_LOG_PARSER_INPUT`| required | Path to input log directory |
|`DIAL_LOG_PARSER_OUTPUT`| required | Path to output log directory |
|`DIAL_LOG_PARSER_DATE`| optional | Date to process logs for (default: yesterday) |
|`DIAL_LOG_PARSER_DEBUG`| optional | Enables debug logging |
|`DIAL_LOG_PARSER_FILENAME_REGEX`| optional | Allows to override the regex to match log file names (default: `date=(\d{4}-\d{2}-\d{2})(\d+)-(\w{8}-\w{4}-\w{4}-\w{4}-\w{12}).log(.gz)?`) |
|`DIAL_LOG_PARSER_INPUT_COMPRESSION`| optional | Compression type for input log files. Possible values: <br/> `infer` - infer compression from file extension (default), <br/> `none` - no compression, <br/> or well known compression types [supported by fsspec](https://filesystem-spec.readthedocs.io/en/latest/features.html#transparent-text-mode-and-compression) (like `gzip`). |
|`DIAL_LOG_PARSER_INPUT_CACHE`| optional | Cache type for input filesystem. Possible values: <br/> `none` - disable caching, <br/> or cache types supported by fsspec (like `readahead`, `bytes`, etc.). <br/> If unset (default), use filesystem specific default caching behavior. <br/> See https://filesystem-spec.readthedocs.io/en/latest/api.html#read-buffering and specific filesystem documentation for details. |

### Storage specific environment variables

Specific storage implementations may require additional environment variables to be set.

For example, for S3, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` may be required. See https://s3fs.readthedocs.io/en/latest/#credentials

Fsspec compatible implementations should be supported (may require to install the extra packages to the docker).
Check the list [Built-in Fsspec Implementations](https://filesystem-spec.readthedocs.io/en/latest/api.html#implementations) and [Other Known Fsspec Implementations](https://filesystem-spec.readthedocs.io/en/latest/api.html#external-implementations) for more details.

#### Azure Blob Storage

For Azure Blob Storage, see [adlfs documentation](https://github.com/fsspec/adlfs?tab=readme-ov-file#setting-credentials) for the list of required environment variables.

**Note**: `AZURE_STORAGE_ANON` should be explicitly set to `false` to use authenticated access. The default value in the adlfs library is `true` which may lead to authentication issues when trying to access private blobs.

If you store the logs compressed as `.logs.gz` and the `Content-Encoding` header for the blob is set to `gzip`, you may encounter an issue where adlfs returns decompressed file content, but reports the file size for the compressed file. This confuses the caching and decompression logic in fsspec and may lead to an error when the parser tries to read the file content.

To work around this issue, you can set the `DIAL_LOG_PARSER_INPUT_COMPRESSION=none` to explicitly disable compression in the parser even if the file name ends with `.gz`, and set `DIAL_LOG_PARSER_INPUT_CACHE=none` to disable caching to avoid issues with the file size mismatch. This way the parser will read the file content as is without trying to decompress it or cache it.

### Command-line arguments
```
Usage: python -m aidial_log_parser.parse_logs [OPTIONS]

  Parse dial log files and repack it to parquet dataset.

Options:
  -i, --input TEXT          Path to input log directory  [env var: DIAL_LOG_PARSER_INPUT; required]
  -o, --output TEXT         Path to output log directory  [env var: DIAL_LOG_PARSER_OUTPUT; required]
  -d, --date [%Y-%m-%d]     Date to process logs for  [env var: DIAL_LOG_PARSER_DATE; default: 2026-02-02]
  --debug                   Enable debug logging  [env var: DIAL_LOG_PARSER_DEBUG]
  --filename-regex TEXT     Regex to match log file names
                            [env var: DIAL_LOG_PARSER_FILENAME_REGEX; default: date=(\d{4}-\d{2}-\d{2})(\d+)-(\w{8}-\w{4}-\w{4}-\w{4}-\w{12}).log(.gz)?]
  --input-compression TEXT  Compression type for input log files.
                            Possible values:
                            'infer' - infer compression from file extension (default),
                            'none' - no compression,
                            or well known compression types supported by fsspec (like 'gzip').
                            [env var: DIAL_LOG_PARSER_INPUT_COMPRESSION]
  --input-cache TEXT        Cache type for input filesystem. Possible values:
                            'default' - use default caching behavior (default),
                            'none' - disable caching,
                            or cache types supported by fsspec (like 'readahead', 'bytes', etc.).
                            If unset (default), use filesystem specific default caching behavior.
                            See https://filesystem-spec.readthedocs.io/en/latest/api.html#read-buffering and specific filesystem documentation
                            for details.
                            [env var: DIAL_LOG_PARSER_INPUT_CACHE]
  --help                    Show this message and exit.
```


## Output format
Output format tries to preserve all the data from the raw logs adding a few columns to help easily access most useful data.

The fields in path:
* **deployment_name** - name of the deployment (e.g. `gpt-35-turbo`, `some-assistant`, `some-application`)
* **date** - date of the log file (e.g. `2023-11-06`)

The fields in the parquet file:
* **request** - structure with the request data. It has the following fields:
  * **uri** - URI of the request
  * **time** - timestamp of the request
  * **body** - string with the body of the request. See the [Dial API documentation](https://dialx.ai/dial_api#operation/sendChatCompletionRequest) for the format of the request body.
* **response** - structure with the response data. It has the following fields:
  * **status** - status code of the response
  * **body** - string with the body of the response. See the [Dial API documentation](https://dialx.ai/dial_api#operation/sendChatCompletionRequest) for the format of the response body.
* **token_usage** - structure with the token usage data. It has the following fields:
  * **prompt_tokens** - number of tokens in the prompt
  * **completion_tokens** - number of tokens in the completion
  * **total_tokens** - total number of tokens in the request
  * **deployment_price** - the cost of this specific request, excluding the cost of any requests it directly or indirectly initiated.
  * **price** - the total cost of the request, including the cost of this request and all related requests it directly or indirectly triggered.
* **assembled_response** - json with assembled response for the chat/completion requests. In case if the request was made with streaming=true, the field will contains an assembled streaming response.
* **question** - last user message in the message history for the chat/completion requests.
* **answer** - string with the application/model response for the chat/completion requests.

The **question** and **answer** fields are not present in the raw logs, but are added to the parquet file for convenience. These fields could simplify the analysis of the logs for a simple applications which do not require a message history or choice of multiple answers.


## Developer environment

This project uses [Python>=3.12](https://www.python.org/downloads/) and [Poetry>=1.8.5](https://python-poetry.org/) as a dependency manager.

Check out Poetry's [documentation on how to install it](https://python-poetry.org/docs/#installation) on your system before proceeding.

To install requirements:

```sh
poetry install
```

This will install all requirements for running the package, linting, formatting and tests.

### IDE configuration

The recommended IDE is [VSCode](https://code.visualstudio.com/).
Open the project in VSCode and install the recommended extensions.

The VSCode is configured to use PEP-8 compatible formatter [Black](https://black.readthedocs.io/en/stable/index.html).

Alternatively you can use [PyCharm](https://www.jetbrains.com/pycharm/).

Set-up the Black formatter for PyCharm [manually](https://black.readthedocs.io/en/stable/integrations/editors.html#pycharm-intellij-idea) or
install PyCharm>=2023.2 with [built-in Black support](https://blog.jetbrains.com/pycharm/2023/07/2023-2/#black).

### Make on Windows

As of now, Windows distributions do not include the make tool. To run make commands, the tool can be installed using
the following command (since [Windows 10](https://learn.microsoft.com/en-us/windows/package-manager/winget/)):

```sh
winget install GnuWin32.Make
```

For convenience, the tool folder can be added to the PATH environment variable as `C:\Program Files (x86)\GnuWin32\bin`.
The command definitions inside Makefile should be cross-platform to keep the development environment setup simple.

## Lint

Run the linting before committing:

```sh
make lint
```

To auto-fix formatting issues run:

```sh
make format
```

## Test

Run unit tests locally:

```sh
make test
```

## Clean

To remove the virtual environment and build artifacts:

```sh
make clean
```
