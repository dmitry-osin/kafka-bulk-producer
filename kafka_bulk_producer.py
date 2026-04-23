#!/usr/bin/env python3
"""
Kafka Bulk Producer

Author: Dmitry Osin <d@osin.pro>

Sends a large number of JSON messages to a Kafka topic.
Messages are generated from a template containing variable placeholders
that are replaced with random values.

Security protocols supported:
  PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import re
import string
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path

# Allow running with vendored dependencies in a local 'vendor' folder.
_VENDOR_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "vendor")
if _VENDOR_PATH not in sys.path and os.path.isdir(_VENDOR_PATH):
    sys.path.insert(0, _VENDOR_PATH)

TEMPLATE_PATTERN = re.compile(
    r"\{(genInt|genFloat|genString|genDate|genTime|genDateTime|genUUID|getFrom)"
    r"(?:\(([^)]*)\))?"
    r"(?:#([^}]+))?\}"
)

_LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")


def _resolve_log_path(log_file: str) -> str:
    if os.path.dirname(log_file):
        return log_file
    os.makedirs(_LOG_DIR, exist_ok=True)
    return os.path.join(_LOG_DIR, log_file)


def _to_strftime_format(fmt: str) -> str:
    return (
        fmt.replace("YYYY", "%Y")
        .replace("MM", "%m")
        .replace("DD", "%d")
        .replace("HH", "%H")
        .replace("mm", "%M")
        .replace("ss", "%S")
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bulk Kafka message producer with template generation."
    )
    parser.add_argument(
        "-log", "--log",
        default="kafka_producer.log",
        help="Path to the log file (default: logs/kafka_producer.log)",
    )
    parser.add_argument(
        "-bootstrap", "--bootstrap",
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument("-topic", "--topic", required=True, help="Target Kafka topic")
    parser.add_argument(
        "-count", "--count",
        type=int,
        required=True,
        help="Total number of messages to send",
    )
    parser.add_argument(
        "-batch", "--batch",
        type=int,
        default=1000,
        help="Number of messages per batch/report cycle",
    )
    parser.add_argument(
        "-template", "--template", required=True, help="Path to the JSON template file"
    )
    parser.add_argument(
        "-transactional-id", "--transactional-id",
        default="",
        help="Transactional ID for the producer (leave empty to disable transactions)",
    )
    parser.add_argument(
        "-security-protocol", "--security-protocol",
        default="PLAINTEXT",
        choices=["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"],
        help="Kafka security protocol",
    )
    parser.add_argument(
        "-sasl-mechanism", "--sasl-mechanism",
        default="PLAIN",
        help="SASL mechanism (e.g. PLAIN, SCRAM-SHA-256)",
    )
    parser.add_argument(
        "-sasl-username", "--sasl-username", default="", help="SASL username"
    )
    parser.add_argument(
        "-sasl-password", "--sasl-password", default="", help="SASL password"
    )
    parser.add_argument(
        "-ssl-ca-location", "--ssl-ca-location",
        default="",
        help="Path to the SSL CA certificate (PEM)",
    )
    parser.add_argument(
        "-ssl-cert-location", "--ssl-cert-location",
        default="",
        help="Path to the client SSL certificate (PEM)",
    )
    parser.add_argument(
        "-ssl-key-location", "--ssl-key-location",
        default="",
        help="Path to the client SSL private key (PEM)",
    )
    parser.add_argument(
        "-ssl-key-password", "--ssl-key-password",
        default="",
        help="Password for the SSL private key",
    )
    return parser.parse_args()


def _setup_logging(log_file: str) -> None:
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def _read_template(template_path: str) -> str:
    global _template_dir
    path = Path(template_path)
    if not path.is_file():
        raise FileNotFoundError(f"Template file not found: {template_path}")
    _template_dir = str(path.parent)
    return path.read_text(encoding="utf-8")


def _build_producer_config(
    bootstrap: str,
    security_protocol: str,
    sasl_mechanism: str,
    sasl_username: str,
    sasl_password: str,
    ssl_ca_location: str,
    ssl_cert_location: str,
    ssl_key_location: str,
    ssl_key_password: str,
    transactional_id: str,
) -> dict:
    config: dict = {
        "bootstrap.servers": bootstrap,
        "security.protocol": security_protocol,
        "batch.size": 16384,
        "linger.ms": 5,
        "compression.type": "snappy",
    }

    if transactional_id:
        config["transactional.id"] = transactional_id

    if security_protocol in ("SASL_PLAINTEXT", "SASL_SSL"):
        config["sasl.mechanisms"] = sasl_mechanism
        config["sasl.username"] = sasl_username
        config["sasl.password"] = sasl_password

    if security_protocol in ("SSL", "SASL_SSL"):
        if ssl_ca_location:
            config["ssl.ca.location"] = ssl_ca_location
        if ssl_cert_location:
            config["ssl.certificate.location"] = ssl_cert_location
        if ssl_key_location:
            config["ssl.key.location"] = ssl_key_location
        if ssl_key_password:
            config["ssl.key.password"] = ssl_key_password

    return config


def _generate_int(length_str: str | None) -> str:
    length = int(length_str) if length_str else 5
    if length < 1:
        return "0"
    lower = 10 ** (length - 1)
    upper = (10**length) - 1
    return str(random.randint(lower, upper))


def _generate_float(length_str: str | None) -> str:
    length = int(length_str) if length_str else 5
    if length < 1:
        return "0.00"
    lower = 10 ** (length - 1)
    upper = (10**length) - 1
    integer_part = random.randint(lower, upper)
    decimal_part = random.randint(0, 99)
    return f"{integer_part}.{decimal_part:02d}"


def _generate_string(length_str: str | None) -> str:
    length = int(length_str) if length_str else 10
    chars = string.ascii_letters + string.digits
    return "".join(random.choices(chars, k=length))


def _generate_date(format_str: str | None) -> str:
    fmt = _to_strftime_format(format_str) if format_str else "%Y-%m-%d"
    days_back = random.randint(0, 365)
    dt = datetime.now() - timedelta(days=days_back)
    return dt.strftime(fmt)


def _generate_time(format_str: str | None) -> str:
    fmt = _to_strftime_format(format_str) if format_str else "%H:%M:%S"
    seconds = random.randint(0, 86399)
    dt = datetime.min + timedelta(seconds=seconds)
    return dt.strftime(fmt)


def _generate_datetime(format_str: str | None) -> str:
    days_back = random.randint(0, 365)
    seconds_back = random.randint(0, 86399)
    dt = datetime.now() - timedelta(days=days_back, seconds=seconds_back)

    if format_str:
        fmt = _to_strftime_format(format_str)
        return dt.strftime(fmt)

    ms = f"{dt.microsecond // 1000:03d}"
    tz = dt.astimezone().strftime("%z")
    if tz:
        tz = tz[:3]
    return f"{dt.strftime('%Y-%m-%dT%H:%M:%S')}.{ms}{tz}"


def _generate_uuid(_: str | None = None) -> str:
    return str(uuid.uuid4())


_file_cache: dict[str, list[str]] = {}
_template_dir: str = ""


def _generate_from_file(path: str, index_str: str | None) -> str:
    if not os.path.isabs(path) and _template_dir:
        resolved = os.path.join(_template_dir, path)
    else:
        resolved = path

    if resolved not in _file_cache:
        p = Path(resolved)
        if not p.is_file():
            raise FileNotFoundError(f"Data file not found: {resolved}")
        lines = p.read_text(encoding="utf-8").splitlines()
        if not lines:
            raise ValueError(f"Data file is empty: {resolved}")
        _file_cache[resolved] = lines
    lines = _file_cache[resolved]
    if index_str is not None:
        idx = int(index_str)
        if idx < 0 or idx >= len(lines):
            raise IndexError(f"Line index {idx} out of range (0-{len(lines)-1})")
        return lines[idx]
    return random.choice(lines)


GENERATORS = {
    "genInt": _generate_int,
    "genFloat": _generate_float,
    "genString": _generate_string,
    "genDate": _generate_date,
    "genTime": _generate_time,
    "genDateTime": _generate_datetime,
    "genUUID": _generate_uuid,
    "getFrom": _generate_from_file,
}


def _replace_variable(match: re.Match) -> str:
    name = match.group(1)
    parens = match.group(2)
    arg = match.group(3)
    generator = GENERATORS.get(name)
    if generator is None:
        raise ValueError(f"Unknown template variable: {name}")
    if parens is not None:
        return generator(parens, arg)
    return generator(arg)


def generate_message(template: str) -> str:
    return TEMPLATE_PATTERN.sub(_replace_variable, template)


def _produce_messages(
    producer,
    topic: str,
    template: str,
    count: int,
    batch: int,
    transactional_id: str,
) -> tuple[int, int, int]:
    sent = 0
    errors = 0
    batches = 0

    def callback(err, msg):
        nonlocal sent, errors
        if err:
            errors += 1
            logging.error("Message delivery failed: %s", err)
            return
        sent += 1

    if transactional_id:
        producer.init_transactions()
        producer.begin_transaction()

    try:
        for i in range(1, count + 1):
            payload = generate_message(template)
            producer.produce(
                topic,
                value=payload.encode("utf-8"),
                callback=callback,
            )

            if i % batch == 0:
                producer.poll(0)
                batches += 1
                logging.info("Progress: %s/%s messages sent", i, count)

        if transactional_id:
            producer.commit_transaction()
    except Exception:
        if transactional_id:
            producer.abort_transaction()
        raise

    producer.flush()
    return sent, errors, batches


def main() -> int:
    args = _parse_args()
    _setup_logging(_resolve_log_path(args.log))

    if args.count <= 0:
        logging.error("Message count must be positive")
        return 1

    try:
        template = _read_template(args.template)
        config = _build_producer_config(
            args.bootstrap,
            args.security_protocol,
            args.sasl_mechanism,
            args.sasl_username,
            args.sasl_password,
            args.ssl_ca_location,
            args.ssl_cert_location,
            args.ssl_key_location,
            args.ssl_key_password,
            args.transactional_id,
        )
    except (FileNotFoundError, ValueError) as exc:
        logging.error("Setup error: %s", exc)
        return 1

    from confluent_kafka import Producer

    producer = Producer(config)

    try:
        sent, errors, batches = _produce_messages(
            producer,
            args.topic,
            template,
            args.count,
            args.batch,
            args.transactional_id,
        )
    except KeyboardInterrupt:
        logging.warning("Interrupted by user")
        producer.flush()
        return 130
    except Exception as exc:
        logging.exception("Production failed: %s", exc)
        return 1

    print(f"\n{'='*50}")
    print(f"Host:              {args.bootstrap}")
    print(f"Topic:             {args.topic}")
    print(f"Total requested:   {args.count}")
    print(f"Batches processed: {batches}")
    print(f"Successfully sent: {sent}")
    print(f"Errors:            {errors}")
    print(f"{'='*50}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
