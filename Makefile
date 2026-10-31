.PHONY: install demo test check spark-test incidents benchmark infra-validate glue-package
install:
	python -m pip install -e '.[dev,aws]'
demo:
	logistics demo --output work/demo --shipments 1000
test:
	pytest -m 'not spark'
check:
	ruff check src tests scripts jobs
	ruff format --check src tests scripts jobs
	mypy src
	python scripts/validate_redshift_sql.py
spark-test:
	SPARK_LOCAL_IP=127.0.0.1 pytest -m spark
incidents:
	logistics incidents --output work/incidents --shipments 12
benchmark:
	logistics benchmark --output work/benchmark --rows 1000000 --repetitions 7
infra-validate:
	terraform -chdir=infra init -backend=false -input=false
	terraform -chdir=infra validate
glue-package:
	python scripts/build_glue_zip.py
