from aiohttp import web

from .server import ClientPool, WSProvider
from .stats import GraphiteStatPusher, StatCollector


def build_app(config) -> web.Application:
    application = web.Application()

    application["config"] = config
    application["stats"] = StatCollector()
    application["clients"] = client_pool = ClientPool()
    application["ws_provider"] = WSProvider(config.secret, client_pool)

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
