# BUILDER STAGE
FROM python:3.10-slim-buster AS builder

RUN apt-get update && apt-get install -y libpq-dev gcc && apt-get install -y libgmp-dev

RUN python -m venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

COPY app/requirements.txt ./

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# OPERATIONAL IMAGE
FROM python:3.10-slim-buster

RUN apt-get update && \
    apt-get install -y libpq-dev && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv

ENV PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH" \
    SCHEMA="https://" \
    DOMAIN="" \
    LN_MIN_AVAIL=1000 \
    BTC_MIN_AVAIL=50000 \
    FEE_LIMIT_SAT=500 \
    LN_MIN_SENDABLE=1000*1000 \
    LN_MAX_SENDABLE=500000*1000 \
    NETWORK=testnet \
    JWT_SECRET=wetassjumper \
    JWT_ALGO=HS256 \
    ACCESS_TOKEN_EXPIRE_MINUTES=720 \
    MACAROON_PATH="" \
    CERT_PATH="" \
    LND_HOST="" \
    WALLET_MASTER_XPUBKEY="" \
    MASTER_0_SIGNER_PUBKEY="" \
    MASTER_1_SIGNER_PUBKEY="" \
    INIT_DATABASE=false \
    DROP_DATABASE=false \
    DOCKER_ENV=false

WORKDIR /app

COPY ./app ./app/

ENV PYTHONPATH=/app/app


CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]