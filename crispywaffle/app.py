from aiohttp import web

from .client import ClientPool
from .stats import GraphiteStatPusher, StatCollector


def build_app(config) -> web.Application:
    application = web.Application()

    application["config"] = config
    application["stats"] = StatCollector()
    application["clients"] = ClientPool()

    application["send_secret"] = config.send_secret
    application["ping_delay"] = config.ping_delay

    if config.graphite:
        stats_root = config.graphite_stats_root or "crispy-waffle"

        pusher = GraphiteStatPusher(
            application['stats'],
            config.graphite,
            stats_root=stats_root,
            frequency=config.graphite_freq
        )

        pusher.start()
    else:
        pusher = None

    application["graphite_pusher"] = pusher

    return application
