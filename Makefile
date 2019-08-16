run:
	go run main.go file.go rto.go protocol.go congestion.go -src=169.254.251.212:19817 -dst=169.254.22.60:19817

rev:
	go run main.go file.go rto.go protocol.go congestion.go -src=169.254.22.60:19817 -dst=169.254.251.212:19817

all:
	go run main.go file.go rto.go protocol.go congestion.go -src=169.254.251.212:19817 -dst=169.254.22.60:19817 \
		-mode=1 -path=checkFiles

.PHONY:
clean:
	rm -f checkFiles/dst/*
	rm -f tmp/file*
