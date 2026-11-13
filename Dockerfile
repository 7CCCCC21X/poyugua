FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PREOPEN_DB=/data/preopen.sqlite3

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py ./
COPY preopen ./preopen
COPY tests ./tests

# The offline suites (no network, no key) run at build time: a failing test fails the build instead of shipping.
RUN python -m compileall -q main.py preopen && python tests/run_all.py && rm -rf tests

CMD ["python", "-u", "main.py"]
