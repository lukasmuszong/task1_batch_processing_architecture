**How to set up locally:**

1. Load the project architecture to the local environment.
2. Set up Docker with sufficient resources.
3. Load the test data sets from kaggle.
     - https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop/data
     - 5 files: 2019-Oct.csv until 2020-Feb.csv
4. Create a folder "/data/new_month/" inside the project architecture.
5. Build the Docker images and run them.
6. Move the first csv file (2019-Oct.csv) into the new_month folder.
6. Log into the airflow interface at: http://localhost:8080/home using the credentials (admin, admin).
7. Run the DAG "sparking_flow" manually.
8. Now you can log into the Grafana interface http://localhost:3000 using the credentials (admin, admin).
9. View the dashboards.
