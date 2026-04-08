FROM python:3.11-slim-bookworm


WORKDIR /src

COPY main.py .
COPY requirements.txt .

RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir --upgrade pip setuptools wheel==0.46.2 jaraco.context==6.1.0 && \
    python -m pip install -r requirements.txt


ENTRYPOINT [ "python", "./main.py" ]