# 🌍 Real-Time Weather Analytics Pipeline

An end-to-end data engineering project to ingest, process, store, and visualize real-time weather data.

## 🔧 Tech Stack

- **Ingestion**: Python, Kafka
- **Streaming**: PySpark Structured Streaming
- **Storage**: S3 (raw/processed), Redshift (analytics)
- **Orchestration**: Apache Airflow
- **Dashboard**: Streamlit

## 🛠️ Pipeline Components

### 1. Ingestion
- `weather_api_producer.py`: Pulls real-time weather data from OpenWeatherMap API and sends to Kafka.

### 2. Streaming Processing
- `weather_stream_processor.py`: Consumes Kafka data, parses and cleans it using PySpark, writes to S3 in Parquet format.

### 3. Data Warehousing
- `load_to_warehouse_dag.py`: Airflow DAG that loads hourly S3 Parquet files into Redshift using `COPY`.

### 4. Dashboard
- `app.py`: Streamlit app that queries Redshift and displays weather trends and raw data.

## 🧪 How to Run

### Kafka
```bash
# Start Kafka locally or with Docker
```

### Ingestion
```bash
python ingestion/weather_api_producer.py
```

### Spark Streaming
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 streaming/weather_stream_processor.py
```

### Airflow
```bash
# Add `load_to_warehouse_dag.py` to your DAGs folder
airflow dags list
airflow dags trigger load_weather_to_redshift
```

### Streamlit
```bash
streamlit run dashboard/app.py
```

## 📦 Requirements
```bash
pip install -r requirements.txt
```

## ✅ Future Enhancements
- Alerting for weather anomalies
- Real-time dashboard with WebSockets
- CI/CD integration
