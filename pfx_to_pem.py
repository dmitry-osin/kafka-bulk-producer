#!/usr/bin/env python3
"""
Convert a PFX/PKCS#12 keystore (Java format) to PEM files for Kafka.

Author: Dmitry Osin <d@osin.pro>

Produces:
  - client certificate (leaf)   -> -ssl-cert-location
  - private key (unencrypted)   -> -ssl-key-location
  - CA chain (issuing CAs)      -> -ssl-ca-location

Requires the 'openssl' command to be available on PATH.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

_CERT_BLOCK = re.compile(
    r"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----",
    re.DOTALL,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert a PFX/PKCS#12 keystore to PEM files."
    )
    parser.add_argument(
        "-in", "--input", required=True, help="Path to the .pfx/.p12 file"
    )
    parser.add_argument(
        "-password", "--password", default="", help="PFX password"
    )
    parser.add_argument(
        "-out-dir", "--output-dir", default="",
        help="Output directory (default: the input file's directory)",
    )
    parser.add_argument(
        "-cert", "--cert-file", default="cert.pem",
        help="Client certificate output name (default: cert.pem)",
    )
    parser.add_argument(
        "-key", "--key-file", default="key.pem",
        help="Private key output name (default: key.pem)",
    )
    parser.add_argument(
        "-ca", "--ca-file", default="ca.pem",
        help="CA chain output name (default: ca.pem)",
    )
    return parser.parse_args()


def _openssl(env: dict[str, str], *args: str) -> str:
    result = subprocess.run(
        ["openssl", *args], capture_output=True, text=True, env=env
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "openssl returned non-zero")
    return result.stdout


def _write(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    print(f"Wrote: {path}")


def main() -> int:
    args = _parse_args()

    src = Path(args.input)
    if not src.is_file():
        print(f"Input file not found: {src}", file=sys.stderr)
        return 1

    out_dir = Path(args.output_dir) if args.output_dir else src.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["PFX_PASSWORD"] = args.password

    try:
        bundle = _openssl(
            env, "pkcs12", "-in", str(src), "-nokeys",
            "-passin", "env:PFX_PASSWORD",
        )
        key = _openssl(
            env, "pkcs12", "-in", str(src), "-nocerts", "-nodes",
            "-passin", "env:PFX_PASSWORD",
        )
    except RuntimeError as exc:
        print(f"Conversion failed: {exc}", file=sys.stderr)
        return 1

    certs = _CERT_BLOCK.findall(bundle)
    if not certs:
        print("No certificates found in the PFX file.", file=sys.stderr)
        return 1

    _write(out_dir / args.cert_file, certs[0] + "\n")

    if len(certs) > 1:
        _write(out_dir / args.ca_file, "\n".join(certs[1:]) + "\n")
    else:
        print("No CA chain found; skipping CA output (only one certificate).")

    _write(out_dir / args.key_file, key)

    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
