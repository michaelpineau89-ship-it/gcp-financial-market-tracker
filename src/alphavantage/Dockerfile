FROM python3.11-alpine

COPY src/alphavantage/main.py /src/main.py
COPY src/alphavantage/requirements.txt /src/requirements.txt

WORKDIR /src

RUN python -m pip --upgrade pip && python -m pip install requirements.txt

ENTRYPOINT [ "python", "./main.py" ]