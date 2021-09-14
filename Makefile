#
#  Makefile
#


SRC = *.py standard_importer worldbank_wdi who_gho un_wpp owid steps tests

default:
	@echo 'Available commands:'
	@echo
	@echo '  make test      Run all linting and unit tests'
	@echo '  make watch     Run all tests, watching for changes'
	@echo

env: requirements.txt
	@echo '==> Updating virtualenv'
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

# jump through some hoops because of a unusual module structure for importers
check-typing: env
	@echo '==> Checking types'
	@cd ..; ./importers/env/bin/mypy \
		--cache-dir=importers/.mypy_cache \
		--config-file=importers/.mypy.ini \
		$$(for part in $(SRC); do echo importers/$$part; done)

unittest: env
	@echo '==> Running unit tests'
	@PYTHONPATH=. env/bin/pytest $(SRC)

format: env
	@echo '==> Reformatting files'
	@env/bin/black $(SRC)

watch: env
	env/bin/watchmedo shell-command -c 'clear; make test' --recursive --drop .
