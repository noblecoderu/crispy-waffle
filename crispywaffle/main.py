#!/usr/bin/env python3

import argparse
import logging
import os

from aiohttp import web

from crispywaffle.routes import setup_routes

from .app import build_app


def load_config():
    parser = argparse.ArgumentParser("Message queue with JWT authentication")

    parser.add_argument(
        "--secret",
        default=os.environ.get("CRISPYWAFFLE_SECRET", None),
        help="JWT secret")

    parser.add_argument("--loglevel", help='Python log level. May be string or number.')
    parser.add_argument("--logformat", help="Python log format")

    parser.add_argument(
        "--graphite",
        default=os.environ.get("CRISPYWAFFLE_GRAPHITE", None),
        help="Graphite stats server host")
    parser.add_argument(
        "--graphite-stats-root",
        help="Graphite stats root key")
    parser.add_argument(
        "--graphite-freq",
        help="Graphite metric collection frequency",
        type=int, default=10
    )

    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", default=8000)

    config = parser.parse_args()

    if not config.secret:
        print("JWT secret not configured!")
        exit(1)

    return config


def run_server() -> None:
    config = load_config()

    logging.basicConfig(
        level=config.loglevel,
        format=config.logformat
    )

    application = build_app(config)
    setup_routes(application)

    web.run_app(application, host=config.host, port=config.port)


if __name__ == "__main__":
    run_server()
