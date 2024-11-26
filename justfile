run:
    cargo r --release -- run manual_testing/check.toml

test:
  cargo nextest run
