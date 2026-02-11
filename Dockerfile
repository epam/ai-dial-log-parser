FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir "poetry==1.8.5"
RUN python3 -m venv /opt/venv

COPY . .
RUN poetry build

ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir dist/aidial_log_parser-*.whl

FROM builder AS test

RUN poetry install --with test
RUN poetry run pytest ./tests

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /

RUN adduser -u 1001 --disabled-password --gecos "" appuser
USER appuser

COPY --from=builder --chown=appuser /opt/venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

ENTRYPOINT ["python", "-m", "aidial_log_parser.parse_logs"]
CMD []
