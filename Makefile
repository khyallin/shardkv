image:
	docker image build -t khyal/shardkv .
	@attempt=1; \
	while [ $$attempt -le 3 ]; do \
		echo "docker push khyal/shardkv (attempt $$attempt/3)"; \
		docker push khyal/shardkv && exit 0; \
		attempt=$$((attempt+1)); \
	done; \
	echo "docker push failed after 3 attempts"; \
	exit 1

all: image