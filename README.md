# Bait Telecommunications Service in Mexico :iphone:

## Owners :busts_in_silhouette:
1. Javier Maldonado
2. Pablo Martínez
3. Román Vélez



## Project Overview :mag:
This project aims to create a robust product data architecture to analyze and visualize the geographical distribution of users utilizing "BAIT" telecommunications service in Mexico. The architecture leverages Python-Dask for an ETL (Extract, Transform, Load) processes, PySpark for additional ELT (Extract, Load, Transform) operations, and Quarto for creating insightful dashboards.

![Project Flow](figures/bait.png)

## Project Structure :open_file_folder:
```
.
├── figures/
├── data/
├── src/
│   ├── s1_etl.py
│   ├── s2_elt.ipynb
│   └── s3_report.qmd
├── README.md
├── requirements.txt
├── config.yaml
└── LICENSE
```

## Prerequisites :scissors:
- AWS account with access to S3, EMR
- Quarto
- Python 3.9+

## Setup and Installation :hammer_and_wrench:

### AWS Configuration :cloud:
1. S3 Buckets: Create S3 buckets for raw data, processed data, and output data.
2. EMR Cluster: Set up an EMR cluster configured to run Spark jobs.
3. IAM Roles: Ensure proper IAM roles and policies are in place to allow access to S3, EMR, and QuickSight.

### Local Environment Setup :computer:
1. Clone the repository

   ```bash
   git clone https://github.com/yourusername/bait-telecom-data-architecture.git
   cd bait-telecom-data-architecture
   ```

2. Set up Python environment:
    ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r elt/requirements.txt
    ```

### ETL Process :arrows_counterclockwise:
#### Dask Process
Dask jobs are used to handle the initial stages of the ETL process.

1. Configuration: Update etl/configs/config.json with your S3 bucket details and other configuration parameters.
2. Submit Dask Jobs: Upload the Dask job scripts to S3 and submit the jobs to your EMR cluster.

   ```bash
   python src/s1_etl.py
   ```

#### ELT Process
PySpark scripts are used for additional data transformation and loading processes.

1. Run the ELT scripts to perform additional transformations and load the data into the output S3 bucket. You must download the data from the raw data S3 bucket to your local environment before running the scripts.
   ```bash
   python src/s2_elt.ipynb
   ```

### Data Visualization :bar_chart:

Quarto is used to create a report for visualizing the geographical distribution of users.

## Create Environment

Create a virtual environment and install the required packages. Using python 3.9+ is recommended.

```bash
conda create -n bait-telecom python=3.9
conda activate bait-telecom
pip install -r requirements.txt
```


## License :page_facing_up:
This project is licensed under the MIT License. See the LICENSE file for details.

For further questions or support, please open an issue in the repository or contact the project maintainer.
