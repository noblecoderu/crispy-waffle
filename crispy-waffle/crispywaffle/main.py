#!/usr/bin/env python3


def load_config():
    import argparse
    import pkg_resources
    import os

    parser = argparse.ArgumentParser("Message queue with JWT authentication")
    parser.set_defaults(post_setup_hooks=[])

    parser.add_argument(
        "--send-secret",
        default=os.environ.get("CRISPYWAFFLE_SEND_SECRET", None),
        help="JWT send secret")
    parser.add_argument(
        "--listen-secret",
        default=os.environ.get("CRISPYWAFFLE_LISTEN_SECRET", None),
        help="JWT listen secret")

    parser.add_argument("--loglevel", help="Python log level. May be string or number.")
    parser.add_argument("--logformat", help="Python log format")
    parser.add_argument(
        "--ping-delay",
        type=int,
        default=10,
        help="Client websocket ping interval")

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

    for entry_point in pkg_resources.iter_entry_points('crispy_waffle_extra_cmd'):
        parser_setter = entry_point.load()
        parser_setter(parser)

    config = parser.parse_args()
    for hook in config.post_setup_hooks:
        hook(config)

    if not config.listen_secret:
        print("JWT listen secret not configured!")
        exit(1)

    if not config.send_secret:
        print("JWT send secret not configured!")
        exit(1)

    return config


def run_server() -> None:
    from .app import build_app
    from .routes import setup_routes
    from aiohttp import web
    import logging

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
