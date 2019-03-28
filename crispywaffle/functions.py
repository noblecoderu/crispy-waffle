from typing import Optional

import jwt
from aiohttp import web

from crispywaffle.logger import CRISPY_LOGGER


def get_signed_data(
        request: web.Request,
        key: str,
        require_exp: Optional[bool] = None
    ) -> dict:
    token: str = request.rel_url.query.get("token")

    if not token:
        CRISPY_LOGGER.debug("Client disconnected, no token")
        raise web.HTTPUnauthorized(text="No token provided")

    try:
        data = jwt.decode(token, key, algorithms=['HS256'])
    except (jwt.DecodeError, jwt.ExpiredSignatureError) as error:
        CRISPY_LOGGER.debug("Client disconnected, invalid token (%s)", error)
        raise web.HTTPBadRequest(text="Invalid token: {}".format(error))

    if require_exp and "exp" not in data:
        CRISPY_LOGGER.debug("Client disconnected, no exp provided")
        raise web.HTTPBadRequest(text="No exp provided")

    return data
