dchan.pb.go: dchan.proto
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative dchan.proto

force:
	rm -f dchan.pb.go
	make dchan.pb.go
