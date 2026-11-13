# python:3.12-slim (Python 3.12.16, built 2026-11-05), pinned by digest: a rebuild gets the same base image. To move it,
# look up the new digest (docker buildx imagetools inspect python:3.12-slim) and run the tests on it.
FROM python:3.12-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PREOPEN_DB=/data/preopen.sqlite3

WORKDIR /app
COPY requirements.lock ./
# every package, transitive ones included, at the locked version and checked against its hash
RUN pip install --no-cache-dir --require-hashes --no-deps -r requirements.lock
COPY main.py ./
COPY preopen ./preopen
COPY tests ./tests

# The offline suites (no network, no key) run at build time: a failing test fails the build instead of shipping.
RUN python -m compileall -q main.py preopen && python tests/run_all.py && rm -rf tests

CMD ["python", "-u", "main.py"]
