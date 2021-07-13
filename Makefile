#
#  Makefile
#


SRC = *.py */

default:
	@echo 'Available commands:'
	@echo
	@echo '  make test      Run all linting and unit tests'
	@echo '  make watch     Run all tests, watching for changes'
	@echo

env: requirements.txt
	test -d env || python -m venv env
	env/bin/pip install -r requirements.txt
	touch env

# check formatting before lint, since an autoformat might fix linting issues
test: check-formatting lint check-typing unittest

lint: env
	@echo '==> Linting'
	@env/bin/flake8 $(SRC)

check-formatting: env
	@echo '==> Checking formatting'
	@env/bin/black --check $(SRC)

check-typing:
	@echo '==> Checking types'
	@env/bin/mypy .

unittest:
	@echo '==> Running unit tests'
	@PYTHONPATH=. env/bin/pytest

format:
	@echo '==> Reformatting files'
	@env/bin/black $(SRC)

watch:
	env/bin/watchmedo shell-command -c 'clear; make test' --recursive --drop .
