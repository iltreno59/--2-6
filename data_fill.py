from faker import Faker
import psycopg2
import random
from datetime import datetime

fake = Faker('ru_RU')

def adapt_phone_number(phone_number):
    phone_number = ''.join(filter(str.isdigit, phone_number))
    phone_number = phone_number.replace(phone_number[0], '8', 1)
    return phone_number

def connect_to_db():
    conn_params = {
    "dbname": "moto_and_cars",
    "user": "postgres",
    "password": "nikita",
    "host": "localhost",
    "port": "5432" 
    }
    try:
        global conn
        conn = psycopg2.connect(**conn_params)
        global cursor
        cursor = conn.cursor()
    except psycopg2.Error as e:
        rint("Ошибка при подключении к базе данных:", e)

def close_conn():
    conn.commit()
    if cursor:
        cursor.close()
    if conn:
        conn.close()
        print("Соединение с базой данных закрыто.")
        
def count_grouped_values(table, column):
    values = []
    select_query=f"""
    SELECT {column}, COUNT(*) FROM {table} GROUP BY {column}
    """
    cursor.execute(select_query)
    for row in cursor:
        values.append(row)
    return dict(values)

def get_all_values_from_table_column(table, column):
    values = []
    select_query = f"""
    SELECT {column} FROM {table}
    """
    cursor.execute(select_query)
    for row in cursor:
        values.append(row)
    return values

def fill_clients():
    for i in range(1500):
        client_data = {
            "name": fake.name(),
            "mail": fake.email(),
            "telephone": adapt_phone_number(fake.phone_number())
        }
        insert_query = """
        INSERT INTO clients(client_name, mail, telephone)
        VALUES
        (%(name)s, %(mail)s, %(telephone)s)
        """
        cursor.execute(insert_query, client_data)
        
def fill_staff():
    for i in range(250):
        staff_data = {
            "name": fake.name(),
            "mail": fake.email(),
            "telephone": adapt_phone_number(fake.phone_number()),
            "role": random.choice(["Администратор", "Аналитик", "Мастер", "Менеджер"]),
            "experience": random.randint(0, 20),
            "birthdate": fake.date_between(start_date=datetime(1970, 1, 1), end_date=datetime(2005, 1, 1)),
            "salary": round(random.randint(30_000, 200_000) / 100) * 100,
            "room_id": random.choice(get_all_values_from_table_column("service", "room_id"))
        }
        insert_query = """
        INSERT INTO staff(employee_name, mail, phone, employee_role, exp, birthday, salary, room_id)
        VALUES
        (%(name)s, %(mail)s, %(telephone)s, %(role)s, %(experience)s, %(birthdate)s, %(salary)s)
        """
        cursor.execute(insert_query, staff_data)

def fill_rooms():
    for i in range(15):
        room_data = {
            "address": fake.address(),
            "phone_number": adapt_phone_number(fake.phone_number()),

        }
        
def main():
    connect_to_db()
    count_grouped_values(table="second_game", column="numorders")
    close_conn()
main()