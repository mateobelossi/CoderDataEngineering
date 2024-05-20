# Running script_Belossi_Pre_Entrega3.py with Docker Compose

## Prerequisites

- Docker installed on your machine. You can download Docker from [here](https://www.docker.com/get-started).

## Getting Started

1. Clone this repository to your local machine:

    ```sh
    git clone https://github.com/mateobelossi/CoderDataEngineering.git
    ```

2. Navigate to the cloned repository and the specific directory:

    ```sh
    cd CoderDataEngineering/Belossi_Tercer_Pre_Entrega
    ```

3. Build and start the Docker container:

    ```sh
    docker-compose build
    docker-compose up
    ```

This will build the Docker image and start the container, which will execute the `script_Belossi_Pre_Entrega3.py` script inside the container using the BashOperator.
