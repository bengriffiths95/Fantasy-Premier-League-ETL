
# Fantasy Premier League ETL & Visualisation

## Overview

Fantasy Premier League (FPL) is a popular online fantasy football game that allows fans to participate in the English Premier League by assuming the role of a manager. You're given a budget of £100 million to sign players to your squad and then decide each week what team you play, with real-life player performances translated into a points system, which then determines how the user performs.

FPL provide an open API - https://fantasy.premierleague.com/api/ - with endpoints for fixture information, scores and player performance. The FPL UI offers limited functionality for analysis, hence the use-case for a more bespoke solution using this openly accessible data.

This project extracts player and fixture data from the API, transforms into a Star schema design and loads into a MySQL database for visualisation with Apache Superset (all using AWS infrastructure - S3, EC2 and RDS)

## Pipeline Architecture
![Architecture Diagram](https://raw.githubusercontent.com/bengriffiths95/Fantasy-Premier-League-ETL/refs/heads/main/Architecture%20Diagram.png)

### Extraction

The data is extracted from three FPL endpoints using the extract.py script. This JSON format data is then saved to the 'Extract' Amazon S3 bucket.

| Endpoint           | Description   |
|--------------------|---------------|
| /bootstrap-static  | Top-level player and team data         |
| /fixtures          | Detailed match fixtures data           |
| /event/{gw}/live   | Detailed player stats per FPL gameweek |


### Transformation

The JSON format data is then transformed into a structured table Star schema using pandas. 
- Separate utility functions for creating the fact and dimension tables can be found in the transform.py file
- Once the data has been transformed, it is saved in a separate S3 bucket in Parquet format

### Loading

Finally, the data is loaded into an Amazon RDS MySQL database with the load.py script. 
- The parquet files are fetched from the S3 bucket, and converted into Pandas DataFrame format
- A connection to the RDS database is established with SQLAlchemy and PyMySQL, with the DB credentials stored as environment variables

### Visualisation

The MySQL RDS database can be connected to any supported visualisation/BI tool for analysis. In this case, Apache Superset was used via [preset.io](https://preset.io/pricing/), which offers a free tier for cloud-hosted dashboards.
## Setup instructions
### Prerequisites

- Python 3.10 or above
- AWS account

### Provisioning AWS Infrastructure (via AWS Console)
##### **1. Set up RDS MySQL Database**
- Navigate to RDS in the AWS Console
- Select 'Create Database', create with the below configuration:
    -  MySQL Engine selected
    -  Selecting 'Free tier' templates and a Free tier-eligible instance, e.g. db.t4g.micro, is recommended to avoid charges in your AWS account
    - Under Additional Configuration, set 'Initial database name' to your choice of name
    - Connectivity:
        - Select 'Don't connect to an EC2 compute resource', and use default VPC and DB subnet group
        - Set public access to 'Yes'
    - All other settings left as default
##### **2. Set up EC2 Instance**
- Navigate to EC2 in the AWS Console
- Select 'Launch Instance', create with the below configuration:
    - Ubuntu 24.x OS Image
    - Instance Type - minimum of t2.small recommended for Airflow (however this is not free tier eligible, so will be charged)
    - Network settings: 
        - ensure same default VPC as used for the RDS DB is selected, and choose the public subnet which is in the same Availability Zone as the RDS DB. 
        - Auto-assign public IP should be set to Yes. 
        - For Firewall (security groups), 'Select existing security group' and use same default as RDS DB.


**Other necessary AWS infrastructure (S3 Buckets, IAM policies) is deployed via Terraform**

### EC2 Instance Setup
##### 1. Connect to EC2 instance via SSH
##### 2. Install Python & sqlite on instance:
    ```
    sudo apt update

    sudo apt install python3-pip

    sudo apt install sqlite3

    sudo apt install python3.10-venv

    sudo apt-get install libpq-dev
    ``` 
##### 3. Clone this repo
    `git clone https://github.com/bengriffiths95/Fantasy-Premier-League-ETL.git`
## License

[MIT](https://choosealicense.com/licenses/mit/)

