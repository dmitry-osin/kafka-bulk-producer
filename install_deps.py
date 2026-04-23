#!/usr/bin/env python3
"""
Install dependencies into a local vendor directory.

Author: Dmitry Osin <d@osin.pro>
"""

import subprocess
import sys


def main() -> int:
    cmd = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "-r",
        "requirements.txt",
        "--target",
        "vendor",
        "--upgrade",
    ]
    print("Installing dependencies into ./vendor ...")
    print(" ".join(cmd))
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        print("Installation failed.", file=sys.stderr)
        return 1
    print("Done. Dependencies are in ./vendor")
    return 0


if __name__ == "__main__":
    sys.exit(main())
