all: help

help:
	@echo "'make docker' - make docker image"
	@echo "'make wheel' - make python wheel"

clean:
	python setup.py clean -a

wheel: clean
	python setup.py bdist_wheel

clean_wheels:
	rm ./wheels/* | true

wheels: clean clean_wheels
	mkdir wheels | true
	pip wheel --wheel-dir ./wheels .

docker: wheels
	docker build --tag crispy-waffle:$(shell python setup.py --version) .
