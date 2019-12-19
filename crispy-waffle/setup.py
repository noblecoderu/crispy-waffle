from setuptools import setup, find_packages


setup(
    name="crispywaffle",
    version="0.3.4",
    maintainer="Eugene Protozanov",
    maintainer_email="protozanov@noblecode.ru",
    description="WebSocket JSON Web Token Message Server",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "aiohttp==3.5.4",
        "PyJWT==1.7.1"
    ],
    extras_require={
        'netconfig': ['boto3', 'requests'],
    },
    entry_points={
        'console_scripts': [
            "crispy-waffle = crispywaffle.main:run_server",
            "crispy-utils = crispywaffle.utils:run_utils"
        ],
    },
)
