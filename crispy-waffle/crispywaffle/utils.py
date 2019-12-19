#!env/bin/python3

import argparse
import json

import jwt


def run_utils() -> None:
    parser = argparse.ArgumentParser("Message queue with JWT authentication")
    parser.add_argument("--secret", dest="secret", required=True)
    parser.add_argument("--sign", dest="message", required=True)

    args = parser.parse_args()
    data = json.loads(args.message)

    print(jwt.encode(data, args.secret.strip(), algorithm="HS256"))


if __name__ == "__main__":
    run_utils()
