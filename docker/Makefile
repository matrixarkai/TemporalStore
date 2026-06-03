NAME = bcache2.compile
VERSION = v1.0
PROJ_HOME = $(shell dirname $(shell realpath .))
PROJ_NAME = $(shell basename $(PROJ_HOME))
.PHONY: all build

all: build

build:
	sed 's/docker\/modulefiles/modulefiles/g' Dockerfile > Dockerfile.tmp
	docker build --force-rm -t $(NAME):$(VERSION) -f Dockerfile.tmp .

run:
	docker run --cap-add SYS_PTRACE -it -v $(PROJ_HOME):/home/tiger/$(PROJ_NAME) --rm $(NAME):$(VERSION) /bin/bash
