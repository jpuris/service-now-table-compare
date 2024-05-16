# service-now-table-compare

## Description

Simple python script that will use the ServiceNow REST API to compare a pair of tables and output the differences.
There can be more than one table pair for script to go through and can be defined via config file.

The script scans the environment files and the config file, then downloads 1 record from each table and
extracts the table fields.

The CSV fieles with table fields are also output in the `data` directory that the script creates. In this directory,
one will find a subdirectory for each environment and a subdirectory for each table pair.

### Keywords

- Python 3.12 or higher
- Poetry
- duckdb
- ServiceNow REST API
- Table comparison

### Config file

The config file is a JSON file with following structure:

```json
{
    "comparison_name_1": {
        "first_env_name": "service_now_table_1",
        "second_env_name": "service_now_table_1"
    },
    "comparison_name_2": {
        "first_env_name": "service_now_table_2",
        "second_env_name": "service_now_table_2"
    }
}
```

### Env file

*.env files are used to describe each environments (instance) name and credentials.

## Prerequisites

1. Python 3.12 or higher
1. Poetry

## Installation

1. The project uses `poetry` for dependency management. To install the dependencies, run:

    Install the dependencies:

    ```sh
        poetry install --no-root
    ```

1. Update the configuration file

    ```sh
        cp config/example.table_map.json table_map.json
    ```

    Then update the `config/table_map.json` with the table pairs you want to compare across the environments.

1. Update the `.env` files with the ServiceNow instance credentials.

    ```sh
        cp -r example.env .env 
    ```

    Then update the secrets for each env in `.env` directory.

## Usage

Activate the local virtual environment:

```sh
    poetry shell
```

To run the script, execute the following command:

```sh
    python main.py
```

Check the `data` directory for the output CSV files and `data/out` for the comparison results.

### Example comparison output

```sh
    $ cat data/out/comparison_name_1.csv
    fn__prod,fn__preprod,exists_in
    ,phone,only in preprod
    ...
    sys_id,sys_id,both
    name,latitude,both
    ...
    u_some_custom_field,,only in prod
```

## License

MIT License

## Contributing

This is a one shot project, so I don't expect any contributions.
