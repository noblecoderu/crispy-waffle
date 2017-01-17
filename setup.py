from setuptools import setup, find_packages


setup(
    name="crispywaffle",
    version="0.1.1",
    maintainer="Eugene Protozanov",
    maintainer_email="protozanov@noblecode.ru",
    description="WebSocket JSON Web Token Message Server",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "aiohttp==1.2.0",
        "PyJWT==1.4.2"
    ],
    entry_points={
        'console_scripts': [
            "crispy-waffle = crispywaffle.server:run_server",
            "crispy-utils = crispywaffle.utils:run_utils"
        ],
    },
)
