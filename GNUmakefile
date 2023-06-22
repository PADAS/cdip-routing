MAKEFLAGS += --warn-undefined-variables

minimum_make_version := 4.1
current_make_version := $(MAKE_VERSION)

ifneq ($(minimum_make_version), $(firstword $(sort $(current_make_version) $(minimum_make_version))))
$(error You need GNU make version $(minimum_make_version) or greater. You have $(current_make_version))
endif

.POSIX:
SHELL := /bin/sh

.DEFAULT_GOAL := help

gcr_root := us-central1-docker.pkg.dev/cdip-78ca/gundi/cdip-routing
semver_file := $(CURDIR)/VERSION

.PHONY: help
help: ## show this help
	@ printf "\033[36m%-20s\033[0m%s\033[0m\n" "target" "description" >&2
	@ printf "%s\n" "------------------------------------------------------------------------" >&2
	@ grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk -F ":.*?## " '{printf "\033[36m%-20s\033[0m%s\033[0m\n", $$1, $$2}' >&2

.PHONY: build_and_push
build_and_push: sha_gcr_tag := $(shell git rev-parse --short --quiet --verify HEAD || exit 1)
build_and_push: ## build and push
	@ if [ -z "$(sha_gcr_tag)" ]; then (printf "\e[31m\tCould not resolve sha_gcr_tag with git rev-parse.\e[0m\n" >&2; exit 1); fi
	docker build --tag $(gcr_root):$(sha_gcr_tag) -f docker/Dockerfile .
	docker push $(gcr_root):$(sha_gcr_tag)
