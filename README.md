# Apache Airflow Repository for Meteo Data ETLs

Welcome to the Apache Airflow Repository for Meteo Data ETLs! This repository is designed to help you perform Extract, Transform, Load (ETL) operations on meteorological data using Apache Airflow, Docker, and PostgreSQL.

## Prerequisites

Before using this repository, ensure that you have the following installed on your system:

- Docker: [Installation Guide](https://docs.docker.com/get-docker/)
- Docker Compose (optional but recommended): [Installation Guide](https://docs.docker.com/compose/install/)
- Apache Airflow CLI (optional): [Installation Guide](https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env.html)

## Getting Started

1. Clone this repository:

```bash
git clone https://github.com/yourusername/airflow-meteo-data-etls.git
```

2. Navigate to the repository's directory:

```bash
cd airflow-meteo-data-etls
```

3. Modify the Airflow configuration, DAGs, and scripts to suit your specific ETL requirements. You can find Airflow configuration in the airflow.cfg file and your ETL DAGs in the dags/ directory.

4. Start Apache Airflow with Docker Compose:

```bash
docker-compose up -d
```

5. Access the Airflow web interface by opening a web browser and visiting http://localhost:8080. Log in with the default username and password: admin/admin.

6. Configure your PostgreSQL database connection in the Airflow web UI (Admin > Connections). You will use this connection to store and retrieve your ETL data.

7. Create and schedule your meteorological data ETL workflows using Apache Airflow's powerful DAGs.
