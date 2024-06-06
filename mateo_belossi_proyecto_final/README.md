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
    cd CoderDataEngineering/Belossi_Tercer_Pre_Entrega
    ```

3. Start the Airflow environment with Docker Compose:

    ```sh
    docker-compose up
    ```

4. Access the Airflow web interface in your browser by visiting [http://localhost:8080](http://localhost:8080).

5. Ensure the necessary Airflow variables are set. Refer to the image below for the required variables configuration:

    ![Airflow Variables](https://github.com/mateobelossi/CoderDataEngineering/blob/main/mateo_belossi_proyecto_final/airflow_variables.png)

6. In the Airflow web interface, enable the `Belossi_Pre_Entrega3` DAG.

7. Trigger the DAG manually or let it run according to its schedule. The DAG will execute the `script_Belossi_Pre_Entrega3.py` script using a BashOperator.

8. Monitor the progress and logs of the DAG execution in the Airflow web interface.

9. After the DAG has completed successfully, you can view the results or any generated output as per the script's functionality.
