SRCS = main.go file.go rto.go protocol.go congestion.go window.go
SRC_ADDR = 169.254.251.212:19817
DST_ADDR = 169.254.22.60:19817
CMD = ./robustp
LOG = point
CONG = vegas
MTU = 1500
OPTS = -log=$(LOG) -cong=$(CONG) -mtu=$(MTU)

run: robustp
	time $(CMD) -src=$(SRC_ADDR) -dst=$(DST_ADDR) $(OPTS)

rev: robustp
	time $(CMD) -src=$(DST_ADDR) -dst=$(SRC_ADDR) $(OPTS)

all: robustp
	$(CMD) -src=$(SRC_ADDR) -dst=$(DST_ADDR) -mode=1 -path=checkFiles $(OPTS)

robustp: $(SRCS)
	go build  -o robustp $(SRCS)

.PHONY:
clean:
	rm -f checkFiles/dst/*
	rm -f tmp/file*
	rm -f robustp
