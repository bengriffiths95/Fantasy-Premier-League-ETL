#################################################################################
#
# Makefile to build project Fantasy-Premier-League-ETL
#
#################################################################################

# Variables
WD=$(shell pwd)
PYTHONPATH=${WD}

# Placeholder db connection variables
rds_db_name = test_database
rds_user = test_user
rds_host = localhost
rds_port = 3306
rds_password = test

# Activate Python virtual environment
ACTIVATE_ENV := source venv/bin/activate

# Execute python related functionalities within correct environment
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

######################## Build ########################

# Create .env file
.PHONY: create-env-file
create-env-file:
	@echo "Creating .env file..."
	@echo "# AWS Region" > .env
	@echo "aws_region='eu-west-2'" >> .env
	@echo "\n# S3 Bucket Names" >> .env
	@echo "s3_extract_bucket_name=placeholder" >> .env
	@echo "s3_transform_bucket_name=placeholder" >> .env
	@echo "\n# EC2 Instance" >> .env
	@echo "ec2_instance_id=placeholder" >> .env
	@echo "\n# RDS Credentials" >> .env
	@echo "rds_db_name=$(rds_db_name)" >> .env
	@echo "rds_user=$(rds_user)" >> .env
	@echo "rds_host=$(rds_host)" >> .env
	@echo "rds_port=$(rds_port)" >> .env
	@echo "rds_password=$(rds_password)" >> .env
	@echo ".env file created successfully."

# Set up EC2 instance
.PHONY: ec2_build
ec2_build:
	bash ./ec2_build.sh

######################## Checks ########################

## Run Black for code formatting
run-black:
	$(call execute_in_env, black  airflow_home/dags/scripts tests)

## Run pytest
unit-test:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest -v)

## Run the coverage check & generate report
check-coverage:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} coverage run -m pytest)
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} coverage report -m)

## Run all checks
run-checks: run-black unit-test check-coverage
