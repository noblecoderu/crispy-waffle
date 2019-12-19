FROM python:3.7

RUN pip install --no-cache-dir --upgrade pip

COPY crispy-waffle/requirements.txt /tmp/requirements.crispy_waffle.txt
COPY crispy-waffle-sentry/requirements.txt /tmp/requirements.crispy_waffle_sentry.txt

RUN pip install --no-cache-dir -r /tmp/requirements.crispy_waffle.txt && \
    pip install --no-cache-dir -r /tmp/requirements.crispy_waffle_sentry.txt && \
    rm /tmp/requirements.crispy_waffle.txt /tmp/requirements.crispy_waffle_sentry.txt

COPY . /tmp/build/
RUN pip install --no-index /tmp/build/crispy-waffle/ /tmp/build/crispy-waffle-sentry/ && rm -r /tmp/build

ENTRYPOINT ["/usr/local/bin/crispy-waffle"]

EXPOSE 8000
