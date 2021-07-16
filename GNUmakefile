MAKEFLAGS += --warn-undefined-variables

minimum_make_version := 4.1
current_make_version := $(MAKE_VERSION)

ifneq ($(minimum_make_version), $(firstword $(sort $(current_make_version) $(minimum_make_version))))
$(error You need GNU make version $(minimum_make_version) or greater. You have $(current_make_version))
endif

.POSIX:
SHELL := /bin/sh

.DEFAULT_GOAL := help

gcr_root := gcr.io/cdip-78ca/cdip-routing
semver_file := $(CURDIR)/VERSION

.PHONY: help
help: ## show this help
	@ printf "\033[36m%-20s\033[0m%s\033[0m\n" "target" "description" >&2
	@ printf "%s\n" "------------------------------------------------------------------------" >&2
	@ grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk -F ":.*?## " '{printf "\033[36m%-20s\033[0m%s\033[0m\n", $$1, $$2}' >&2

.PHONY: build_and_push
build_and_push: descriptive_gcr_path_stem := $(shell git config --get user.name || exit 1)
build_and_push: descriptive_gcr_path := $(gcr_root)/$(shell echo $(descriptive_gcr_path_stem) | sed 's/[^/A-Za-z0-9_-]/_/g' | tr '[:upper:]' '[:lower:]' || exit 1)
build_and_push: latest_gcr_tag := latest
build_and_push: semver_gcr_tag := $(shell cat "$(semver_file)" || exit 1)
build_and_push: sha_gcr_tag := $(shell git rev-parse --quiet --verify HEAD || exit 1)
build_and_push: unique_gcr_tag := $(shell date +%s || exit 1)
build_and_push: ## build and push
	@ if [ -z "$(semver_gcr_tag)" ]; then (printf "\e[31m\tCould not resolve semver_gcr_tag by cat'ing '$(semver_file)'.\e[0m\n" >&2; exit 1); fi
	@ if [ -z "$(sha_gcr_tag)" ]; then (printf "\e[31m\tCould not resolve sha_gcr_tag with git rev-parse.\e[0m\n" >&2; exit 1); fi
	@ if [ -z "$(unique_gcr_tag)" ]; then (printf "\e[31m\tCould not resolve unique_gcr_tag with 'date'.\e[0m\n" >&2; exit 1); fi
	@ if [ "$$(dirname $(descriptive_gcr_path))" = "$$(dirname $(gcr_root))" ]; then (printf "\e[31m\tCould not construct a unique, sanitized GCR path.\e[0m\n" >&2; exit 1); fi
	docker build --tag $(descriptive_gcr_path):$(latest_gcr_tag) --tag $(descriptive_gcr_path):$(semver_gcr_tag) --tag $(descriptive_gcr_path):$(sha_gcr_tag) --tag $(descriptive_gcr_path):$(unique_gcr_tag) -f docker/Dockerfile .
	docker push $(descriptive_gcr_path):$(latest_gcr_tag)
	docker push $(descriptive_gcr_path):$(semver_gcr_tag)
	docker push $(descriptive_gcr_path):$(sha_gcr_tag)
	docker push $(descriptive_gcr_path):$(unique_gcr_tag)

