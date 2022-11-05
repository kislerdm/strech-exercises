tests:
	@ docker run \
 		-v $(PWD):/src \
 		jointask:test pytest --cov=main -vv .

lint:
	@ docker run \
     		-v $(PWD):/src \
     		jointask:test isort *.py && mypy *.py && flake8 *.py && black -l 120 *.py
