{
  description = "Haskell resource pool";

  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-21.11";
    flake-compat = {
      url = "github:edolstra/flake-compat";
      flake = false;
    };
  };

  outputs = { self, flake-utils, nixpkgs, ... }:
    let
      systemAttrs = flake-utils.lib.eachDefaultSystem (system:
        let
          compiler = "ghc8107";
          pkgs = nixpkgs.legacyPackages."${system}".extend self.overlay;
          ghc = pkgs.haskell.packages."${compiler}";
        in {

          apps.repl = flake-utils.lib.mkApp {
            drv =
              nixpkgs.legacyPackages."${system}".writeShellScriptBin "repl" ''
                confnix=$(mktemp)
                echo "builtins.getFlake (toString $(git rev-parse --show-toplevel))" >$confnix
                trap "rm $confnix" EXIT
                nix repl $confnix
              '';
          };

          pkgs = pkgs;

          devShell = ghc.shellFor {
            withHoogle = false;
            nativeBuildInputs = with pkgs; [ cabal-install ];
            packages = hpkgs:
              with hpkgs;
              with pkgs.haskell.lib;
              [ (doCheck demeter) ];
          };

          packages = { demeter = ghc.demeter; };

          defaultPackage = ghc.demeter;
          checks = pkgs.lib.attrsets.genAttrs [ "ghc8107" "ghc901" "ghc921" ]
            (ghc-ver: pkgs.haskell.packages."${ghc-ver}".demeter);
        });
      topLevelAttrs = {
        overlay = final: prev: {
          haskell = with prev.haskell.lib;
            prev.haskell // {
              packages = let
                ghcs = prev.lib.filterAttrs
                  (k: v: prev.lib.strings.hasPrefix "ghc" k)
                  prev.haskell.packages;
                patchedGhcs = builtins.mapAttrs patchGhc ghcs;
                patchGhc = k: v:
                  prev.haskell.packages."${k}".extend (self: super:
                    with prev.haskell.lib;
                    with builtins;
                    with prev.lib.strings;
                    let
                      cleanSource = pth:
                        let
                          src' = prev.lib.cleanSourceWith {
                            filter = filt;
                            src = pth;
                          };
                          filt = path: type:
                            let isHiddenFile = hasPrefix "." (baseNameOf path);
                            in !isHiddenFile;
                        in src';
                    in {
                      demeter = let
                        p =
                          self.callCabal2nix "demeter" (cleanSource ./src) { };
                      in p;
                    });
              in prev.haskell.packages // patchedGhcs;
            };
        };
      };
    in systemAttrs // topLevelAttrs;
}
