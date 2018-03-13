FROM python:3.6-slim

COPY wheels /tmp/wheels/

RUN pip install /tmp/wheels/*.whl

CMD ["crispy-waffle"]

EXPOSE 8080
