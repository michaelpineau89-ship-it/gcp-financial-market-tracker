FROM python:3.11-alpine


WORKDIR /src

COPY main.py .
COPY requirements.txt .

RUN python -m pip install --upgrade pip && \
    python -m pip install -r requirements.txt

ENTRYPOINT [ "python", "./main.py" ]