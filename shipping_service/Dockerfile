FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY shippingservice.py shippingservice.py

CMD ["python", "shippingservice.py"]
