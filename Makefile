VERSION ?= 0.0.1
IMAGE ?= subgraph-converter
REPO ?= <your-repo>

.PHONY: docker-buildx
docker-buildx:
	docker buildx build --push --platform linux/arm64 --tag "$(REPO)/$(IMAGE):$(VERSION)" .
