from setuptools import setup, find_packages


setup(
    name="crispywaffle",
    version="0.3",
    maintainer="Eugene Protozanov",
    maintainer_email="protozanov@noblecode.ru",
    description="WebSocket JSON Web Token Message Server",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "aiohttp~=3.0.0",
        "aioredis==1.1.0",
        "aioh2==0.2.2",
        "PyJWT==1.5.3",
        "PyYAML==3.12",
    ],
    entry_points={
        'console_scripts': [
            "crispy-waffle = crispywaffle.server:run_server",
            "crispy-utils = crispywaffle.utils:run_utils"
        ],
    },
)
