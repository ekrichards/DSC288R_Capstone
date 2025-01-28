.PHONY: all data clean setup lint test help

# Default target
all: data

# Run the DVC pipeline
data:
	dvc repro

# Clean intermediate and processed files
clean:
	rm -rf raw/extracted data/processed

# Install dependencies
setup:
	pip install -r requirements.txt

# Lint the codebase
lint:
	flake8 src/

# Run tests
# test:
# 	pytest tests/

# Show help information
help:
	@echo "Available commands:"
	@echo "  make        Run the entire pipeline (default)"
	@echo "  make data   Run the DVC pipeline"
	@echo "  make clean  Clean intermediate and processed data"
	@echo "  make setup  Install project dependencies"
	@echo "  make lint   Lint the codebase"
	@echo "  make test   Run tests"
