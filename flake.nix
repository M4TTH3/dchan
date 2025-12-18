{
  description = "Modern Go development environment (no GOPATH)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
      in
      {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            go                # compiler
            gopls             # LSP
            gotools           # goimports, godoc, etc.
            delve             # debugger
            air               # live reload
            gotests           # generate table-driven tests
            impl              # generate interface stubs

            protobuf
            protoc-gen-go
            protoc-gen-go-grpc
          ];

          shellHook = ''
            export GOPATH=$PWD/.go
            export PATH=$GOPATH/bin:$PATH
          '';
        };
      }
    );
}
