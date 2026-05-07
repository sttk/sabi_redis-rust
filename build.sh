#!/usr/bin/env bash

errcheck() {
  exitcd=$1
  if [[ "$exitcd" != "0" ]]; then
    exit $exitcd
  fi
}

clean() {
  cargo clean
  errcheck $?
}

format() {
  cargo fmt
  errcheck $?
}

lint() {
  cargo clippy --all-features
  errcheck $?
}

compile() {
  cargo build --all-features
  errcheck $?
}

test() {
  ulimit -n 8192

  echo "### features: default (= standalone)"
  cargo test -- --show-output
  errcheck $?

  echo "### features: standalone-async"
  cargo test --features standalone-async --no-default-features -- --show-output
  errcheck $?

  echo "### features: sentinel"
  cargo test --features sentinel --no-default-features -- --show-output
  errcheck $?

  echo "### features: sentinel-async"
  cargo test --features sentinel-async --no-default-features -- --show-output
  errcheck $?

  echo "### features: cluster"
  cargo test --features cluster --no-default-features -- --show-output
  errcheck $?

  echo "### features: cluster-async"
  cargo test --features cluster-async --no-default-features -- --show-output
  errcheck $?
}

unit() {
  cargo test --all-features -- --show-output $1
  errcheck $?
}

cover() {
  cargo llvm-cov clean
  errcheck $?
  cargo llvm-cov --all-features --html --quiet
  errcheck $?
  cargo llvm-cov report
  errcheck $?
}

bench() {
  cargo +nightly bench --quiet -- $1
  errcheck $?
}

doc() {
  cargo +nightly rustdoc --all-features -- --cfg docsrs
  errcheck $?
}

msrv() {
  cargo msrv find --all-features --ignore-lockfile --no-check-feedback
  errcheck $?
}

if [[ "$#" == "0" ]]; then
  #clean
  format
  compile
  test
  lint
  doc
  cover

elif [[ "$1" == "unit" ]]; then
  unit $2

else
  for a in "$@"; do
    case "$a" in
    clean)
      clean
      ;;
    format)
      format
      ;;
    compile)
      compile
      ;;
    test)
      test
      ;;
    lint)
      lint
      ;;
    doc)
      doc
      ;;
    cover)
      cover
      ;;
    bench)
      bench
      ;;
    msrv)
      msrv
      ;;
    *)
      echo "Bad task: $a"
      exit 1
      ;;
    esac
  done
fi
