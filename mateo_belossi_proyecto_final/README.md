# Running Airflow DAG with Docker Compose

## Prerequisites

- Docker installed on your machine. You can download Docker from [here](https://www.docker.com/get-started).
- Airflow environment configured with Docker Compose. You can use the configuration provided in this repository.

## Getting Started

1. Clone this repository to your local machine:

    ```sh
    git clone https://github.com/mateobelossi/CoderDataEngineering.git
    ```

2. Navigate to the cloned repository and the specific directory:

    ```sh
    cd CoderDataEngineering/mateo_belossi_proyecto_final
    ```

3. Start the Airflow environment with Docker Compose:

    ```sh
    docker-compose up
    ```

    If you need to rebuild the images, use:

    ```sh
    docker-compose build
    docker-compose up
    ```

    Or, to force a rebuild of the images before starting the containers:

    ```sh
    docker-compose up --build
    ```

4. Access the Airflow web interface in your browser by visiting [http://localhost:8080](http://localhost:8080). Use the following credentials:

   - Username: `airflow`
   - Password: `airflow`

5. Ensure the necessary Airflow variables are set. Refer to the image below for the required variables configuration:

    ![Airflow Variables](https://github.com/mateobelossi/CoderDataEngineering/blob/main/mateo_belossi_proyecto_final/airflow_variables.png)

6. In the Airflow web interface, enable the `mateo_belossi_proyecto_final` DAG.

7. Trigger the DAG manually or let it run according to its schedule. The DAG will execute the `mateo_belossi_proyecto_final.py` script using a BashOperator.

8. Monitor the progress and logs of the DAG execution in the Airflow web interface.

9. After the DAG has completed successfully, you can view the results or any generated output as per the script's functionality.

## Functionality

- First source of data: make a request to the Binance API using the endpoint: [https://api.binance.com/api/v1/ticker/24hr](https://api.binance.com/api/v1/ticker/24hr).
- Include a column "created_at" to df in the obtained results. (symbol and created_at is the composite key)
- Clean data and drop duplicates from the response received from the Binance API.
- Delete from Redshift any rows where created_at equals the current day of execution to prevent duplicates.
- Insert data from API to Redshift.
- Second source of data: read a JSON file from [alerts/alerts.json](https://github.com/mateobelossi/CoderDataEngineering/blob/main/mateo_belossi_proyecto_final/alerts/alerts.json) where for each coin_pair there is a min_price and a max_price.
- Using the function check_price_alerts, it checks for price alerts for each coin pair in the dataframe and returns a single text with all results.
  Returns None if there are no alerts.
- If there exists any alerts then send an email with the body of all the alerts detected.
