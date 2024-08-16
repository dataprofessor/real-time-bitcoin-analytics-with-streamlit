import logging
import psycopg2


POSTGRESQL_HOST = "database"
POSTGRESQL_PORT = 5432
POSTGRESQL_DATABASE = "quix"
POSTGRESQL_USER = "user"
POSTGRESQL_PASSWORD = "password"


def main():
    client = psycopg2.connect(
                        host=POSTGRESQL_HOST,
                        port=POSTGRESQL_PORT,
                        database=POSTGRESQL_DATABASE,
                        user=POSTGRESQL_USER,
                        password=POSTGRESQL_PASSWORD,
    )
    
    cursor = client.cursor()

    cursor.execute("SELECT * FROM ohlc")

    records = cursor.fetchall()
    for record in records:
        print(record)
    
    logging.debug(records)


if __name__ == "__main__":
    try:
        logging.basicConfig(level="DEBUG")
        main()
    except KeyboardInterrupt:
        print("Exiting")