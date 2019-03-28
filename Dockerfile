FROM python:3.7

RUN pip install --no-cache-dir --upgrade pip

COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm -rf /tmp/requirements.txt

COPY . /tmp/build/
RUN pip install --no-index /tmp/build && rm -rf /tmp/build

ENTRYPOINT ["/usr/local/bin/crispy-waffle"]

EXPOSE 8000
