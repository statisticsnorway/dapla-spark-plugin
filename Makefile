.PHONY: default
default: | help

.PHONY: build-mvn
build-mvn: ## Build the project and install to you local maven repo
ifndef skipTest
	mvn clean install
else
	mvn clean install -Dmaven.test.skip=true
endif

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-45s\033[0m %s\n", $$1, $$2}'
