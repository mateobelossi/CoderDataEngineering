# Pre-Entrega Script

This Jupyter Notebook demonstrates how to interact with the Binance API, process the obtained data, save it into a CSV file, insert it into Redshift, and read the data from Redshift.

## Functionality

- Makes requests to the Binance API using the endpoint: [https://api.binance.com/api/v1/ticker/24hr](https://api.binance.com/api/v1/ticker/24hr).
- Include a column "created_at" to df in the obtained results. (symbol and created_at is the composite key)
- Clean data and drop duplicates from the response received from the Binance API.
- Saves the results in a CSV file.
- Delete from Redshift any rows where created_at equals the current day of execution to prevent duplicates.
- Insert data from API to into Redshift.
- Reads the data from Redshift for further processing and analysis.

## Jupyter Notebook

The notebook containing first preliminary delivery script can be found [here](https://github.com/mateobelossi/CoderDataEngineering/blob/main/EntregableBelossi.ipynb).

The notebook containing second preliminary delivery script can be found [here](https://github.com/mateobelossi/CoderDataEngineering/blob/main/EntregableBelossi_Segunda_Pre_Entrega.ipynb).

## CSV Output

The CSV file from first preliminary delivery can be found [here](https://github.com/mateobelossi/EntregableBelossi.ipynb/blob/main/20240401_180736_mercado_binance.csv).

The CSV file from second preliminary delivery can be found [here](https://github.com/mateobelossi/CoderDataEngineering/blob/main/20240425_021723_mercado_binance.csv).


