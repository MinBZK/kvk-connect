from __future__ import annotations

import argparse

from kvk_connect import KVKApiClient
from kvk_connect.utils import get_env


def run():
    """Start de CLI tool voor KVK Connect."""

    parser = argparse.ArgumentParser(prog="kvk-connect")
    parser.add_argument("--kvk", help="KVK number", required=True)
    parser.add_argument("--geo", action="store_true", default=True)
    args = parser.parse_args()

    api_key = get_env("KVK_API_KEY_PROD", required=True) or "NO_KEY_FOUND"
    client = KVKApiClient(api_key)
    try:
        basis = client.get_basisprofiel(args.kvk, geo_data=args.geo)
        if not basis:
            print("No data")
            return
        print(basis.to_dict())
    finally:
        client.close()
