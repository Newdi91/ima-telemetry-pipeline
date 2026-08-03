# IMA Telemetry Medallion Pipeline

A Medallion architecture data engineering pipeline (Bronze, Silver, Gold) built with **Databricks Asset Bundles (DAB)** and executed on **Databricks Serverless Compute**.

## 🏗️ Pipeline Architecture (DAG)
The workflow consists of sequential steps followed by a parallel branch for the Gold layer:
1. **Setup (`00_setup_infrastructure.py`)**: Initializes catalogs, schemas, and base tables.
2. **Mock Data (`01_generate_mock_data.py`)**: Generates simulated telemetry data using the `Faker` library.
3. **Bronze (`02_bronze_ingestion.py`)**: Raw data ingestion layer.
4. **Silver (`03_silver_transformation.py`)**: Data cleansing, normalization, and structuring.
5. **Gold Performance (`04_gold_performance.py`)**: Performance metrics aggregation.
6. **Gold Mechanical (`05_gold_mechanical.py`)**: Mechanical metrics aggregation.

## 🚀 Prerequisites & Setup
- [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) installed and configured with the `DEFAULT` profile (PAT authentication).
- Python 3.10+

### Local Environment Setup
```bash
pip install -r requirements.txt
