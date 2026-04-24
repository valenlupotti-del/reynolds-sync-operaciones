FROM python:3.11-slim

WORKDIR /app

RUN pip install requests

COPY sync_contacts.py .

RUN echo "0 8 * * * cd /app && python sync_contacts.py >> /var/log/sync.log 2>&1" > /etc/cron.d/sync
RUN chmod 0644 /etc/cron.d/sync
RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*

CMD cron && tail -f /var/log/sync.log
