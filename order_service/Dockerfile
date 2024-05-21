FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY orderservice.py orderservice.py

CMD ["python", "orderservice.py"]
