# Pre-Entrega Script

This Jupyter Notebook demonstrates how to interact with the Binance API, process the obtained data, save it into a CSV file, insert it into Redshift, and read the data from Redshift.

## Functionality

- Makes requests to the Binance API using the endpoint: [https://api.binance.com/api/v1/ticker/24hr](https://api.binance.com/api/v1/ticker/24hr).
- Includes a column named "created_at" in the obtained results, indicating the current time at the moment of making the requests.
- Saves the results in a CSV file.
- Inserts the results into Redshift.
- Reads the data from Redshift for further processing and analysis.

## Jupyter Notebook

The notebook containing the script can be found [here](https://github.com/mateobelossi/EntregableBelossi.ipynb/blob/main/pre_entrega_script.ipynb).

## CSV Output

The CSV file containing the processed data can be found [here]([path_to_csv_file.csv](https://github.com/mateobelossi/EntregableBelossi.ipynb/blob/main/20240401_125605_mercado_binance.csv)).


