import psycopg2
from psycopg2 import OperationalError

def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database = db_name,
            user = db_user,
            password = db_password,
            host = db_host,
            port = db_port,
        )
        print("Connection to boodac_test as successful")
    except OperationalError as e:
        print(f"The error '{e}' as occured")
    return connection

def create_activites():
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("""CREATE TABLE IF NOT EXISTS activites(
                id SERIAL PRIMARY KEY,
                code_ape VARCHAR,
                activite_insee TEXT,
                code_secteur INTEGER,
                Libelle VARCHAR
            );""")


def create_forme_juridiques():
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("""CREATE TABLE IF NOT EXISTS forme_juridiques(
                id SERIAL PRIMARY KEY,
                forme_juridique VARCHAR
            );""")

def create_localisations():
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("""CREATE TABLE IF NOT EXISTS localisations(
                id SERIAL PRIMARY KEY,
                longitude FLOAT,
                latitude FLOAT,
                code_postal INTEGER,
                departement VARCHAR,
                region VARCHAR,
                ville VARCHAR
            );""")

def create_entite():
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("""CREATE TABLE IF NOT EXISTS entites(
                id SERIAL PRIMARY KEY,
                siren VARCHAR,
                activite_declaree TEXT,
                fk_date_creation INTEGER,
                fk_date_radiation INTEGER,
                fk_localisation INTEGER,
                fk_forme_juridique INTEGER,
                fk_insee INTEGER,
                CONSTRAINT fk_creation FOREIGN KEY (fk_date_creation) REFERENCES days(id),
                CONSTRAINT fk_radiation FOREIGN KEY (fk_date_radiation) REFERENCES days(id),
                CONSTRAINT fk_localisation FOREIGN KEY (fk_localisation) REFERENCES  localisations(id),
                CONSTRAINT fk_forme_juridique FOREIGN KEY (fk_forme_juridique) REFERENCES forme_juridiques(id),
                CONSTRAINT fk_insee FOREIGN KEY (fk_insee) REFERENCES activites(id)
            );""")


def create_years():
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("""CREATE TABLE IF NOT EXISTS years(
                id SERIAL PRIMARY KEY,
                year INTEGER
            );""")

def create_months():
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("""CREATE TABLE IF NOT EXISTS months(
                id SERIAL PRIMARY KEY,
                fk_year INTEGER,
                CONSTRAINT fk_year FOREIGN KEY (fk_year) REFERENCES years(id)
            );""")

def create_days():
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("""CREATE TABLE IF NOT EXISTS days(
                id SERIAL PRIMARY KEY,
                fK_month INTEGER,
                day INTEGER,
                CONSTRAINT fk_month FOREIGN KEY (fk_month) REFERENCES months(id)
            );""")


def insert_activites(code_ape, activite_insee, division, division_insee):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("INSERT INTO activites(code_ape, activite_insee, division, division_insee) VALUES(%s, %s, %s, %s);",(code_ape, activite_insee, division, division_insee))



connection = create_connection("bodacc_test","steeven2","toto","127.0.0.1","5432")

create_activites()
create_forme_juridiques()
create_localisations()
create_years()
create_months()
create_days()
create_entite()
# insert_activites()