SRCS = main.go file.go rto.go protocol.go congestion.go window.go
SRC_ADDR = 169.254.251.212:19817
DST_ADDR = 169.254.22.60:19817

run:
	go run $(SRCS) -src=$(SRC_ADDR) -dst=$(DST_ADDR)

rev:
	go run $(SRCS) -src=$(DST_ADDR) -dst=$(SRC_ADDR)

all:
	go run $(SRCS) -src=$(SRC_ADDR) -dst=$(DST_ADDR) \
		-mode=1 -path=checkFiles

.PHONY:
clean:
	rm -f checkFiles/dst/*
	rm -f tmp/file*
