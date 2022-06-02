.PHONY: clean all

help:
	@echo "Targets in Makefile:"
	@cat Makefile

init:
	pip install -r requirements.txt

dev:
	pip install -e .

run: init dev
	event_processing

test: init dev
	pytest --tb=short

clean-build:
	find . -name '*.egg-info' -exec rm -fr {} +

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyd' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test: clean-pyc
	find . -name '*.pytest_cache' -exec rm -fr {} +

clean: clean-build clean-pyc clean-test

all: clean init dev run