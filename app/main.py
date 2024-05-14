import os
import csv
import json

from pathlib import Path

import requests
import duckdb


def load_dotenv(dotenv_path: str):
    """Load secrets from a .env file into the environment variables."""
    with open(dotenv_path, "r") as f:
        for line in f:
            key, value = line.strip().split("=")
            os.environ[key] = value


def download_attr_names(
        env: str,
        table_name: str,
        data_dir: str,
):
    """Download attribute names for a table from ServiceNow API and write to a CSV file."""
    # Load secrets from .env file
    env_file_path = Path(__file__).parent / f"../.env/{env}.env"
    load_dotenv(env_file_path)
    secrets = {
            "instance_name": os.getenv("SN_API_INSTANCE_NAME"),
            "user": os.getenv("SN_API_USER"),
            "pass": os.getenv("SN_API_PASS"),
    }

    # Get attribute names
    url = f"https://{secrets['instance_name']}.service-now.com/api/now/table/{table_name}"
    params = {
        "sysparm_limit": 1,
        "sysparm_exclude_reference_link": True
    }
    response = requests.get(
        url,
        params=params,
        auth=(secrets["user"], secrets["pass"]))
    response.raise_for_status()

    attr_names = list(response.json()["result"][0].keys())
    attr_names.sort()
    
    # write to a CSV file with attribute_names as header
    output_file_path = Path(data_dir) / f"{env}/{table_name}.csv"
    with open(output_file_path, "w") as f:
        writer = csv.writer(f)
        writer.writerow(['attr_name'])
        for attr_name in attr_names:
            writer.writerow([attr_name])


def load_csv(
        dbcon,
        file_path: str,
        env: str,
        table_name: str,
):
    """Load a CSV file into a duckdb table."""
    query = f"""
    CREATE TABLE {env}__{table_name} AS
    SELECT attr_name
    FROM read_csv_auto('{file_path}');
    """
    dbcon.execute(query)

def compare_datasets(
        dbcon,
        data_dir: str,
        job_name: str,
        job_data: dict,
):
    """Test the CSV files for common rows and write to a CSV file."""
    # Load the table pair CSV files into duckdb
    for env, table_name in job_data.items():
        file_path = Path(data_dir) / f"{env}/{table_name}.csv"
        load_csv(dbcon, file_path, env, table_name)

    # # Find common rows
    query_params = [
        {
            "env": env,
            "entity_name": table_name,
            "table_name": f"{env}__{table_name}"
        } for env, table_name in job_data.items()
    ]
    read_query = f"""
    SELECT
        tbl1.attr_name AS fn__{query_params[0]['env']},
        tbl2.attr_name AS fn__{query_params[1]['env']},
        CASE
            WHEN tbl1.attr_name IS NULL THEN 'only in {query_params[1]['env']}'
            WHEN tbl2.attr_name IS NULL THEN 'only in {query_params[0]['env']}'
            ELSE 'both'
        END AS exists_in
    FROM {query_params[0]['table_name']} AS tbl1
    FULL OUTER JOIN {query_params[1]['table_name']} AS tbl2
        ON tbl1.attr_name = tbl2.attr_name
    ORDER BY CONCAT(tbl1.attr_name, tbl2.attr_name)
    """

    output_dir = Path(data_dir) / "out"
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / f"{job_name}.csv"

    write_out = f"""
    COPY ({read_query}) TO '{output_file}' (HEADER, DELIMITER ',');
    """

    print(f"Writing output to {output_file}")
    dbcon.execute(write_out)


def refresh_data_files(table_map: str):
    """Refresh the data files for all the tables in the table_map."""
    for task_name, table_pair in table_map.items():
        for env, table_name in table_pair.items():
            print(f"Downloading attribute names for '{table_name}', environment: '{env}'")
            download_attr_names(env, table_name, data_dir)
        print(f"Data refreshed for {task_name}")


if __name__ == "__main__":

    refresh_enabled = True

    data_dir = Path(__file__).parent / "../data"
    config_dir = Path(__file__).parent / "../config"

    # Create data directory if it doesn't exist
    data_dir.mkdir(exist_ok=True)

    # Table map
    table_map_filename = config_dir / "table_map.json"
    table_map = json.loads(table_map_filename.read_text())

    if refresh_enabled:
        refresh_data_files(table_map)

    dbcon = duckdb.connect(":memory:")
    for job_name, job_data in table_map.items():
        compare_datasets(dbcon, data_dir, job_name, job_data)
