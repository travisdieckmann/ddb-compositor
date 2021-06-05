target:
	$(info ${HELP_MESSAGE})
	@exit 0
	
init:
	pip install -e '.[dev]'

test:
	pytest --cov ddb_compositor --cov-report term-missing --cov-fail-under 80 tests/*

black:
	black setup.py ddb_compositor/* tests/*

black-check:
	black --check setup.py ddb_compositor/* tests/*

# Command to run everytime you make changes to verify everything works
dev: test

# Verifications to run before sending a pull request
pr: black-check init dev

define HELP_MESSAGE

Usage: $ make [TARGETS]

TARGETS
	init        Initialize and install the requirements and dev-requirements for this project.
	test        Run the Unit tests.
	dev         Run all development tests after a change.
	pr          Perform all checks before submitting a Pull Request.

endef