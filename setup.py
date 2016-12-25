from setuptools import setup, find_packages


setup(
    name="crispywaffle",
    version="0.0.1",
    maintainer="Eugene Protozanov",
    maintainer_email="protozanov@noblecode.ru",
    description="WebSocket JSON Web Token Message Server",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "aiohttp",
        "pyjwt"
    ],
    entry_points={
        'console_scripts': [
            "crispy-waffle = crispywaffle.server:run"
        ],
    },
)
