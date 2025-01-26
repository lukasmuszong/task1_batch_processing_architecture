from airflow.models import Connection
from sqlalchemy.exc import IntegrityError
from airflow.settings import Session

session = Session()

conn_id = "spark-conn"
existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()

if existing_conn is None:
    new_conn = Connection(
        conn_id=conn_id,
        conn_type="spark",
        description="Connection to the Spark master node for ETL processing",
        host="spark://sparkingflow-spark-master-1",
        port=7077,
    )
    session.add(new_conn)
    try:
        session.commit()
        print("Connection added successfully.")
    except IntegrityError as e:
        session.rollback()
        print(f"Failed to add connection: {e}")
else:
    print(f"Connection with conn_id '{conn_id}' already exists.")