FROM python:3.11-slim

WORKDIR /app

RUN pip install flask requests

COPY . .

EXPOSE 5001

CMD ["python", "webhook_operaciones.py"]
