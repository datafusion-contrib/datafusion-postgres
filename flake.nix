{
  description = "Development environment flake";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, fenix, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        pythonEnv = pkgs.python3.withPackages (ps: with ps; [
          psycopg2-binary
          psycopg
          pyarrow
        ]);
        buildInputs = with pkgs; [
          llvmPackages.libclang
          libpq
          duckdb.dev
          duckdb.lib
        ];
      in
      {
        devShells.default = pkgs.mkShell {
          nativeBuildInputs = with pkgs; [
            pkg-config
            clang
            git
            mold
            (fenix.packages.${system}.stable.withComponents [
              "cargo"
              "clippy"
              "rust-src"
              "rustc"
              "rustfmt"
              "rust-analyzer"
            ])
            cargo-nextest
            cargo-release
            curl
            pythonEnv
            postgresql.out
          ];

          LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath buildInputs;
          shellHook = ''
            export CC=clang
            export CXX=clang++
            export DUCKDB_LIB_DIR="${pkgs.duckdb.lib}/lib"
            export DUCKDB_INCLUDE_DIR="${pkgs.duckdb.dev}/include"
          '';
        };
      });
}
