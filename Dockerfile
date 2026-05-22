# Dockerfile - Content Store Cloud Jobs
#
# Build context: monorepo root (contains content_store/, infra/).

FROM python:3.13-slim

WORKDIR /app

# Layer 1: shared internal package
COPY infra/ /tmp/infra/
RUN pip install --no-cache-dir /tmp/infra && rm -rf /tmp/infra

# Layer 2: job dependencies
COPY content_store/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Layer 3: job source
COPY content_store/ ./content_store/

CMD ["python", "-m", "content_store.run"]
