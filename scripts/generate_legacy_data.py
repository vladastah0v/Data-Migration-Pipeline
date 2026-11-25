import sys

from migration_pipeline.cli import app

if __name__ == "__main__":
    app(["seed", *sys.argv[1:]])
