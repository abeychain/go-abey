# This Makefile is meant to be used by people that do not usually work
# with Go source code. If you know what GOPATH is then you probably
# don't need to bother with make.

.PHONY: gabey deps android ios gabey-cross swarm evm all test clean
.PHONY: gabey-linux gabey-linux-386 gabey-linux-amd64 gabey-linux-mips64 gabey-linux-mips64le
.PHONY: gabey-linux-arm gabey-linux-arm-5 gabey-linux-arm-6 gabey-linux-arm-7 gabey-linux-arm64
.PHONY: gabey-darwin gabey-darwin-386 gabey-darwin-amd64
.PHONY: gabey-windows gabey-windows-386 gabey-windows-amd64

GOBIN = $(shell pwd)/build/bin
GO ?= latest
DEPS = $(shell pwd)/internal/jsre/deps

gabey:
	build/env.sh go run build/ci.go install ./cmd/gabey
	@echo "Done building."
	@echo "Run \"$(GOBIN)/gabey\" to launch gabey."

genkey:
	$(GORUN) build/ci.go install ./cmd/genKey
	@echo "Done building."
	@echo "Run \"$(GOBIN)/genKey\" to launch genKey."

deps:
	cd $(DEPS) &&	go-bindata -nometadata -pkg deps -o bindata.go bignumber.js web3.js
	cd $(DEPS) &&	gofmt -w -s bindata.go
	@echo "Done generate deps."

swarm:
	build/env.sh go run build/ci.go install ./cmd/swarm
	@echo "Done building."
	@echo "Run \"$(GOBIN)/swarm\" to launch swarm."

all:
	build/env.sh go run build/ci.go install

# android:
#	build/env.sh go run build/ci.go aar --local
#	@echo "Done building."
#	@echo "Import \"$(GOBIN)/gabey.aar\" to use the library."

# ios:
#	build/env.sh go run build/ci.go xcode --local
#	@echo "Done building."
#	@echo "Import \"$(GOBIN)/Gabey.framework\" to use the library."

test: all
	build/env.sh go run build/ci.go test

lint: ## Run linters.
	build/env.sh go run build/ci.go lint

clean:
	rm -fr build/_workspace/pkg/ $(GOBIN)/*

# The devtools target installs tools required for 'go generate'.
# You need to put $GOBIN (or $GOPATH/bin) in your PATH to use 'go generate'.

devtools:
	env GOBIN= go get -u golang.org/x/tools/cmd/stringer
	env GOBIN= go get -u github.com/kevinburke/go-bindata/go-bindata
	env GOBIN= go get -u github.com/fjl/gencodec
	env GOBIN= go get -u github.com/golang/protobuf/protoc-gen-go
	env GOBIN= go install ./cmd/abigen
	@type "npm" 2> /dev/null || echo 'Please install node.js and npm'
	@type "solc" 2> /dev/null || echo 'Please install solc'
	@type "protoc" 2> /dev/null || echo 'Please install protoc'

# Cross Compilation Targets (xgo)

gabey-cross: gabey-linux gabey-darwin gabey-windows gabey-android gabey-ios
	@echo "Full cross compilation done:"
	@ls -ld $(GOBIN)/gabey-*

gabey-linux: gabey-linux-386 gabey-linux-amd64 gabey-linux-arm gabey-linux-mips64 gabey-linux-mips64le
	@echo "Linux cross compilation done:"
	@ls -ld $(GOBIN)/gabey-linux-*

gabey-linux-386:
	build/env.sh go run build/ci.go xgo -- --go=$(GO) --targets=linux/386 -v ./cmd/gabey
	@echo "Linux 386 cross compilation done:"
	@ls -ld $(GOBIN)/gabey-linux-* | grep 386

gabey-linux-amd64:
	build/env.sh go run build/ci.go xgo -- --go=$(GO) --targets=linux/amd64 -v ./cmd/gabey
	@echo "Linux amd64 cross compilation done:"
	@ls -ld $(GOBIN)/gabey-linux-* | grep amd64

gabey-linux-arm: gabey-linux-arm-5 gabey-linux-arm-6 gabey-linux-arm-7 gabey-linux-arm64
	@echo "Linux ARM cross compilation done:"
	@ls -ld $(GOBIN)/gabey-linux-* | grep arm

gabey-linux-arm-5:
	build/env.sh go run build/ci.go xgo -- --go=$(GO) --targets=linux/arm-5 -v ./cmd/gabey
	@echo "Linux ARMv5 cross compilation done:"
	@ls -ld $(GOBIN)/gabey-linux-* | grep arm-5

gabey-linux-arm-6:
	build/env.sh go run build/ci.go xgo -- --go=$(GO) --targets=linux/arm-6 -v ./cmd/gabey
	@echo "Linux ARMv6 cross compilation done:"
	@ls -ld $(GOBIN)/gabey-linux-* | grep arm-6

gabey-linux-arm-7:
	build/env.sh go run build/ci.go xgo -- --go=$(GO) --targets=linux/arm-7 -v ./cmd/gabey
	@echo "Linux ARMv7 cross compilation done:"
	@ls -ld $(GOBIN)/gabey-linux-* | grep arm-7

gabey-linux-arm64:
	build/env.sh go run build/ci.go xgo -- --go=$(GO) --targets=linux/arm64 -v ./cmd/gabey
	@echo "Linux ARM64 cross compilation done:"
	@ls -ld $(GOBIN)/gabey-linux-* | grep arm64

gabey-linux-mips:
	build/env.sh go run build/ci.go xgo -- --go=$(GO) --targets=linux/mips --ldflags '-extldflags "-static"' -v ./cmd/gabey
	@echo "Linux MIPS cross compilation done:"
	@ls -ld $(GOBIN)/gabey-linux-* | grep mips

gabey-linux-mipsle:
	build/env.sh go run build/ci.go xgo -- --go=$(GO) --targets=linux/mipsle --ldflags '-extldflags "-static"' -v ./cmd/gabey
	@echo "Linux MIPSle cross compilation done:"
	@ls -ld $(GOBIN)/gabey-linux-* | grep mipsle

gabey-linux-mips64:
	build/env.sh go run build/ci.go xgo -- --go=$(GO) --targets=linux/mips64 --ldflags '-extldflags "-static"' -v ./cmd/gabey
	@echo "Linux MIPS64 cross compilation done:"
	@ls -ld $(GOBIN)/gabey-linux-* | grep mips64

gabey-linux-mips64le:
	build/env.sh go run build/ci.go xgo -- --go=$(GO) --targets=linux/mips64le --ldflags '-extldflags "-static"' -v ./cmd/gabey
	@echo "Linux MIPS64le cross compilation done:"
	@ls -ld $(GOBIN)/gabey-linux-* | grep mips64le

gabey-darwin: gabey-darwin-386 gabey-darwin-amd64
	@echo "Darwin cross compilation done:"
	@ls -ld $(GOBIN)/gabey-darwin-*

gabey-darwin-386:
	build/env.sh go run build/ci.go xgo -- --go=$(GO) --targets=darwin/386 -v ./cmd/gabey
	@echo "Darwin 386 cross compilation done:"
	@ls -ld $(GOBIN)/gabey-darwin-* | grep 386

gabey-darwin-amd64:
	build/env.sh go run build/ci.go xgo -- --go=$(GO) --targets=darwin/amd64 -v ./cmd/gabey
	@echo "Darwin amd64 cross compilation done:"
	@ls -ld $(GOBIN)/gabey-darwin-* | grep amd64

gabey-windows: gabey-windows-386 gabey-windows-amd64
	@echo "Windows cross compilation done:"
	@ls -ld $(GOBIN)/gabey-windows-*

gabey-windows-386:
	build/env.sh go run build/ci.go xgo -- --go=$(GO) --targets=windows/386 -v ./cmd/gabey
	@echo "Windows 386 cross compilation done:"
	@ls -ld $(GOBIN)/gabey-windows-* | grep 386

gabey-windows-amd64:
	build/env.sh go run build/ci.go xgo -- --go=$(GO) --targets=windows/amd64 -v ./cmd/gabey
	@echo "Windows amd64 cross compilation done:"
	@ls -ld $(GOBIN)/gabey-windows-* | grep amd64
