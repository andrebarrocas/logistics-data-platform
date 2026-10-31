"""AWS Glue 5.0 entrypoint; supply dist/logistics_platform.zip via --extra-py-files."""

from logistics_platform.spark_job import main

if __name__ == "__main__":
    main()
