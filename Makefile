# Self-Documented Makefile see https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html

.DEFAULT_GOAL := help

# Put it first so that "make" without argument is like "make help".
help:
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-32s-\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: help

run-sample: ## Build clean and run sample project
	./gradlew clean build runProject -P chooseMain=SetupSQSSNS

run-sqs:  ## Build clean and run com.aws.sqs project
	./gradlew clean build runProject -P chooseMain=SQS

