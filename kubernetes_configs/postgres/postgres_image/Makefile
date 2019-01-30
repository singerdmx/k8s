GOOGLE_CLOUD_PROJECT:=$(shell gcloud config list project --format="value(core.project)")

.PHONY: build
build:
	docker build -t gcr.io/$(GOOGLE_CLOUD_PROJECT)/postgres-pw .

.PHONY: push
push: build
	gcloud docker push gcr.io/$(GOOGLE_CLOUD_PROJECT)/postgres-pw

