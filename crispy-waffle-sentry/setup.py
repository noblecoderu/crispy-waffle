from setuptools import setup, find_packages


setup(
    name="crispywaffle-sentry",
    version="0.3.4",
    maintainer="Eugene Protozanov",
    maintainer_email="protozanov@noblecode.ru",
    description="Sentry handler for crispy-waffle message queue",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "sentry-sdk==0.13.5",
    ],
    entry_points={
        'crispy_waffle_extra_cmd': [
            'sentry = crispywaffle_sentry.main:setup_argument_parsers'
        ],
    },
)
