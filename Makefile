image:
	sudo docker image build -t khyallin/shardkv .
	docker push khyallin/shardkv

all: image