IMAGE_NAME ?= ai-dial-log-parser
PLATFORM ?= linux/amd64
POETRY ?= poetry
DOCKER ?= docker
ARGS ?=


.PHONY: all install build clean install_nox lint format test docker_build docker_test

all: build


install:
	$(POETRY) install


build: install
	$(POETRY) build


clean:
	nox -s clean
	$(POETRY) env remove --all


install_nox:
	$(POETRY) install --only nox


lint: install_nox
	$(POETRY) run nox -s lint


format: install_nox
	$(POETRY) run nox -s format


test: install_nox
	$(POETRY) run nox -s test $(ARGS)


docker_build:
	$(DOCKER) build --platform $(PLATFORM) -t $(IMAGE_NAME):dev .


docker_test:
	$(DOCKER) build --platform $(PLATFORM) --target test .
