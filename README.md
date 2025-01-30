**How to set up locally:**

 1. Load the project architecture to the local environment.
 2. Set up Docker with sufficient resources.
 3. Load the test data sets from kaggle.
     - https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop/data
     - 5 files: 2019-Oct.csv until 2020-Feb.csv
 4. Create a folder "/data/new_month/" inside the project architecture.
 5. Move the first csv file (2019-Oct.csv) into the new_month folder.
 6. Build the Docker images and run them.
 7. Log into the airflow interface at: http://localhost:8080/home using the credentials (admin, admin).
 8. Run the DAG "sparking_flow" manually.
 9. Now you can log into the Grafana interface http://localhost:3000 using the credentials (admin, admin).
10. View the dashboards.



Steps:



- brew install gh
- brew install docker docker-compose
- set up system resources in docker to include 10 CPUs and 10 GB of memory.
- gh auth login
- gh repo clone lukasmuszong/task1_batch_processing_architecture
- mkdir data data/original data/new_month
- #!/bin/bash
curl -L -o ./data/ecommerce-events-history-in-cosmetics-shop.zip\
  https://www.kaggle.com/api/v1/datasets/download/mkechinov/ecommerce-events-history-in-cosmetics-shop
- unzip data/ecommerce-events-history-in-cosmetics-shop.zip

- docker compose build
- docker compose up -d

- log into the airflow portal at http://localhost:8080/home
- User Credentials are admin:admin
- move the first test data month into the new month
    - mv data/original/2019-Oct.csv data/new_month

- now manually trigger the DAG from the airflow portal

- Now you can log into the Grafana interface http://localhost:3000 using the credentials (admin, admin) and view the dashboards.

- next, the following monthly data (Nov-2020) could be moved into the "new_month" folder and the dag could be triggered again. In a production set up the file would have to reside in the folder as the new month is starting, which is when airflow schedules the DAG to run.
