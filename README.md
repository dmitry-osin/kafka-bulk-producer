# Kafka Bulk Producer

**Author:** Dmitry Osin <d@osin.pro>  
**License:** MIT

A high-throughput Python script for generating and sending large numbers of text messages to Apache Kafka. Messages are created from a template containing variable placeholders that are replaced with random values at runtime. The template can be any text file — JSON, XML, CSV, or plain text.

---

## Features

- Asynchronous message production using `confluent-kafka`
- Text template engine with random value generators
- Progress logging with configurable batch size
- Local dependency vendoring (no global `pip install` required)
- Multiple security protocols: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
- Transactional producer support

---

## Requirements

- Python 3.10+
- Network access to a Kafka broker

---

## Installation

Install dependencies into a local `vendor` folder:

```bash
python install_deps.py
```

This downloads `confluent-kafka` and places it next to the script.

---

## Usage

```bash
python kafka_bulk_producer.py \
  -topic my-topic \
  -count 400000 \
  -batch 5000 \
  -template template.json \
  -bootstrap kafka.example.com:9093 \
  -security-protocol SASL_SSL \
  -sasl-username user \
  -sasl-password pass \
  -ssl-ca-location ca-cert.pem
```

### Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `-bootstrap` | Kafka bootstrap servers | `localhost:9092` |
| `-topic` | Target Kafka topic | *(required)* |
| `-count` | Total number of messages to send | *(required)* |
| `-batch` | Number of messages per reporting cycle | `1000` |
| `-template` | Path to the JSON template file | *(required)* |
| `-log` | Path to the log file | `logs/kafka_producer.log` |
| `-transactional-id` | Transactional ID for the producer | *(empty = disabled)* |
| `-security-protocol` | Security protocol (PLAINTEXT / SSL / SASL_PLAINTEXT / SASL_SSL) | `PLAINTEXT` |
| `-sasl-mechanism` | SASL mechanism (e.g. PLAIN, SCRAM-SHA-256) | `PLAIN` |
| `-sasl-username` | SASL username | *(optional)* |
| `-sasl-password` | SASL password | *(optional)* |
| `-ssl-ca-location` | Path to the SSL CA certificate (PEM) | *(optional)* |
| `-ssl-cert-location` | Path to the client SSL certificate (PEM) | *(optional)* |
| `-ssl-key-location` | Path to the client SSL private key (PEM) | *(optional)* |
| `-ssl-key-password` | Password for the SSL private key | *(optional)* |

---

## SSL / SASL Notes

- **PEM format**: this tool uses the `confluent-kafka` client (based on `librdkafka`), which works with **PEM certificates**, not JKS. If you have JKS files, convert them to PEM first using `keytool` or `openssl`.
- For **PLAINTEXT** no extra settings are needed.
- For **SSL** you must provide at least the CA certificate (`-ssl-ca-location`). Client certificate and key are only required if the broker enforces mutual TLS (mTLS).
- For **SASL_PLAINTEXT** provide `-sasl-username` and `-sasl-password`.
- For **SASL_SSL** provide both SASL credentials and SSL certificates.

---

## Template Placeholders

The JSON template supports dynamic placeholders that are replaced with random values for every message.

| Placeholder | Example | Description |
|-------------|---------|-------------|
| `{genInt#length}` | `{genInt#6}` | Random integer with the specified number of digits |
| `{genFloat#length}` | `{genFloat#4}` | Random float; `length` sets the integer-part digit count (2 decimal places are always added) |
| `{genString#length}` | `{genString#12}` | Random alphanumeric string of the given length |
| `{genDate#format}` | `{genDate#YYYY-MM-DD}`, </br> `{genDate}` | Random date within the last year |
| `{genTime#format}` | `{genTime#HH:mm:ss}`, </br> `{genTime}` | Random time of day |
| `{genDateTime#format}` | `{genDateTime#YYYY-MM-DD HH:mm:ss}`, </br> `{genDateTime}` | Random date and time within the last year |
| `{genUUID}` | `{genUUID}` | — | Random UUID v4 |

### Format tokens for dates

When providing a custom format, use these tokens (case-sensitive):

| Token | Meaning | Python equivalent |
|-------|---------|-------------------|
| `YYYY` | 4-digit year | `%Y` |
| `MM` | 2-digit month | `%m` |
| `DD` | 2-digit day | `%d` |
| `HH` | 24-hour hour | `%H` |
| `mm` | 2-digit minute | `%M` |
| `ss` | 2-digit second | `%S` |

### Template Example

Create a any text file:

```json
{
  "id": "{genUUID}",
  "user_id": {genInt#6},
  "amount": {genFloat#4},
  "currency": "USD",
  "reference": "{genString#12}",
  "transaction_date": "{genDateTime#YYYY-MM-DD HH:mm:ss}",
  "effective_date": "{genDate#YYYY-MM-DD}",
  "processed_time": "{genTime#HH:mm:ss}",
  "arrive_date_time": "{genDateTime}",
  "arrive_date": "{genDate}",
  "arrive_time": "{genTime}",
  "card_id": "{getFrom(card_ids.txt)#6}"
}
```

Example output with default formats:

```json
{
  "id": "e4a1b3da-9abc-4e56-b532-7852c5133eb3",
  "user_id": 713694,
  "amount": 6912.61,
  "currency": "USD",
  "reference": "IAvCBhnFinFx",
  "transaction_date": "2025-08-15 14:32:07",
  "effective_date": "2025-08-15",
  "processed_time": "01:45:02",
  "arrive_date_time": "2025-08-15T14:32:07.042+03",
  "arrive_date": "2025-08-15",
  "arrive_time": "01:45:02",
  "card_id": "1234-4567-8901-0000"
}
```

Every message produced will have unique random values for each placeholder.

---

## License

This project is licensed under the [MIT License](LICENSE).

## Notes

- `confluent-kafka` contains platform-specific compiled extensions. If you move the project to another OS (e.g., from Windows to Linux), rerun `python install_deps.py` on the target machine.
- The script polls for delivery reports after every batch, so memory usage stays flat even with hundreds of thousands of messages.
