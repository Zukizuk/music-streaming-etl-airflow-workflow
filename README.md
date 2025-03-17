# Music Streaming ETL Pipeline

![Architecture Diagram](/assets/Architecture.png)

## Project Overview

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline for a music streaming service using Apache Airflow. The pipeline processes streaming data and metadata to generate key performance indicators (KPIs) for business intelligence. You can see here [Solution Document](/assets/Technical%20Solution%20Document.pdf) to
read more on it.

## Features

- **Data Extraction**: Ingests user and song metadata from Amazon RDS (simulated via CSV) and streaming data from Amazon S3.
- **Data Validation**: Ensures data integrity by verifying required columns.
- **Data Transformation**: Cleans and transforms raw streaming data.
- **KPI Computation**:
  - _Genre-Level KPIs_: Listen Count, Average Track Duration, Popularity Index, Most Popular Track per Genre.
  - _Hourly KPIs_: Unique Listeners, Top Artists per Hour, Track Diversity Index.
- **Data Loading**: Optimized Upsert strategy to efficiently load transformed data into Amazon Redshift.
- **Error Handling & Logging**: Implements error handling and logging mechanisms.

## Tech Stack

- **Orchestration**: Apache Airflow
- **Storage**: Amazon S3, Amazon Redshift, Amazon RDS and Redis (Temp Data)
- **Processing**: Python, Pandas

## Architecture

1. **Extraction**: Data is extracted from Amazon S3 and RDS.
2. **Validation**: Ensures all required columns exist in datasets.
3. **Transformation**: Cleans, processes, and computes KPIs.
4. **Loading**: Stores processed data into Amazon Redshift for analysis.

## Installation & Setup

### Prerequisites

- Python 3.7+
- Apache Airflow
- AWS credentials configured for S3, Redshift access and Redis credentials

### Installation Steps

1. Clone the repository:
   ```bash
   git clone https://github.com/Zukizuk/music-streaming-etl-airflow-workflow
   cd music-streaming-etl-airflow-workflow
   ```
2. Set up a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Configure Airflow:
   ```bash
   export AIRFLOW_HOME=$(pwd)/airflow
   airflow db init
   airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com
   ```
5. Start Airflow:
   ```bash
   airflow scheduler &
   airflow webserver &
   ```
6. Deploy the DAG:
   - Copy the `music_streaming_etl.py` DAG file to the `dags/` directory of Airflow.
   - Enable the DAG in the Airflow UI.

## Usage

- The pipeline runs on a scheduled interval (daily) or can be triggered manually via the Airflow UI.
- Processed data is stored in Redshift for analysis using SQL queries.

## DAG Workflow

![Dag Run](/assets/Dag%20Taskflow.png)

## Future Implementation

- **Improve Logging**: Add more informative logging to track DAG performance and potential issues.
- **Error Handling**: Implement a robust error handling system to catch and handle exceptions.
- **Monitoring**: Integrate with monitoring tools such as Prometheus and Grafana to monitor DAG performance.
- **Security**: Implement encryption for sensitive data and use AWS Secret Manager for secure key management.

## Contribution

1. Fork the repository.
2. Create a feature branch:
   ```bash
   git checkout -b feature-branch
   ```
3. Commit your changes:
   ```bash
   git commit -m "Add new feature"
   ```
4. Push to your branch:
   ```bash
   git push origin feature-branch
   ```
5. Open a pull request.

## License

This project is licensed under the MIT License.

## Contact

For any inquiries, please contact **Marzuk Entsie** at marzuk.entsie@amalitech.com.
