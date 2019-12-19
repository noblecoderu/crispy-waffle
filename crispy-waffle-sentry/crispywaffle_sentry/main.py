import argparse
import sentry_sdk
from sentry_sdk.integrations.aiohttp import AioHttpIntegration
from sentry_sdk.utils import BadDsn


def setup_sentry(namespace: argparse.Namespace):
    try:
        sentry_sdk.init(
            dsn=namespace.sentry_dsn,
            integrations=[AioHttpIntegration()]
        )
    except BadDsn as error:
        print(f"Failed to setup sentry: {error}")


def setup_argument_parsers(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--sentry",
        dest="post_setup_hooks",
        action="append_const",
        const=setup_sentry
    )
    parser.add_argument(
        "--sentry-dsn",
        dest="sentry_dsn",
        type=str
    )

