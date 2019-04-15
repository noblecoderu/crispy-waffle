from typing import TYPE_CHECKING

from . import views

if TYPE_CHECKING:  # pragma: no cover
    from aiohttp import web


def setup_routes(application: "web.Application"):
    application.router.add_route('GET', '/message', application["ws_provider"].ws_connect)
    application.router.add_route('POST', '/message', views.send_message)
    application.router.add_route('DELETE', '/user/{uid}', views.remove_user)
    application.router.add_route('GET', '/user', views.short_user_info)
    application.router.add_route('POST', '/match', views.match_user)
