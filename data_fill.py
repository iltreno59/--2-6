from faker import Faker
import psycopg2
import random
from datetime import datetime
import conn_options

fake = Faker('ru_RU')

def adapt_phone_number(phone_number):
    phone_number = ''.join(filter(str.isdigit, phone_number))
    phone_number = phone_number.replace(phone_number[0], '8', 1)
    phone_number = phone_number.replace(' ', '')
    return phone_number

def connect_to_db():
    conn_params = {
    "dbname": conn_options.db_name,
    "user": conn_options.user_name,
    "password": conn_options.password,
    "host": conn_options.host,
    "port": conn_options.port 
    }
    try:
        global conn
        conn = psycopg2.connect(**conn_params)
        global cursor
        cursor = conn.cursor()
    except psycopg2.Error as e:
        print("Ошибка при подключении к базе данных:", e)

def close_conn():
    conn.commit()
    if cursor:
        cursor.close()
    if conn:
        conn.close()
        print("Соединение с базой данных закрыто.")
        
def count_grouped_values(table, column):
    values = []
    select_query = f"""
    SELECT {column}, COUNT(*) FROM {table} GROUP BY {column}
    """
    cursor.execute(select_query)
    for row in cursor:
        values.append(row)
    return dict(values)

def count_table_lines(table):
    value = 0
    select_query = f"""
    SELECT COUNT(*) FROM {table}
    """
    cursor.execute(select_query)
    for row in cursor:
        value = row[0]
    return value 

def get_first_value_from_column(table, column):
    value = 0
    select_query = f"""
    SELECT {column} FROM {table} LIMIT 1
    """
    cursor.execute(select_query)
    for row in cursor:
        value = row[0]
    return value

def get_last_value_from_column(table, column):
    value = 0
    select_query = f"""
    SELECT {column} FROM {table} ORDER BY {column} DESC LIMIT 1 
    """
    cursor.execute(select_query)
    for row in cursor:
        value = row[0]
    return value      

def get_all_values_from_table_column(table, column):
    values = []
    select_query = f"""
    SELECT {column} FROM {table}
    """
    cursor.execute(select_query)
    for row in cursor:
        values.append(row)
    return values

def get_all_managers_id():
    values = []
    select_query = f"""
    SELECT employee_id FROM staff WHERE employee_role = 'Менеджер'
    """
    cursor.execute(select_query)
    for row in cursor:
        values.append(row)
    return values
def get_all_masters_id():
    values = []
    select_query = f"""
    SELECT employee_id FROM staff WHERE employee_role = 'Мастер'
    """
    cursor.execute(select_query)
    for row in cursor:
        values.append(row)
    return values

def get_all_masters_from_room(room_id):
    values = []
    select_query = f"""
    SELECT staff.employee_id FROM staff 
    LEFT JOIN staff_in_rooms ON staff_in_rooms.employee_id = staff.employee_id
    WHERE room_id = {room_id[0]}
    ORDER BY employee_id
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
            "room_id": random.choice(get_all_values_from_table_column("rooms", "room_id"))
        }
        insert_query = """
        INSERT INTO staff(employee_name, mail, phone, employee_role, exp, birthday, salary, room_id)
        VALUES
        (%(name)s, %(mail)s, %(telephone)s, %(role)s, %(experience)s, %(birthdate)s, %(salary)s, %(room_id)s)
        """
        cursor.execute(insert_query, staff_data)

def fill_rooms():
    for i in range(15):
        auto_bool = random.choice(["TRUE", "FALSE"])
        room_data = {
            "addres": ' '.join(fake.address().split(', ')[1::]),
            "phone_number": adapt_phone_number(fake.phone_number()),
            "auto": auto_bool,
            "moto": ("TRUE" if auto_bool == "FALSE" else random.choice(["TRUE", "FALSE"])),
            "city": fake.city(),
            "post_index": fake.postcode()
        }
        insert_query = """
        INSERT INTO rooms(addres, phone_number, auto, moto, city, post_index)
        VALUES
        (%(addres)s, %(phone_number)s, %(auto)s, %(moto)s, %(city)s, %(post_index)s)
        """
        cursor.execute(insert_query, room_data)


def fill_details_in_rooms():
            for j in range(get_first_value_from_column("rooms", "room_id"), get_last_value_from_column("rooms", "room_id") + 1):
                for k in range(get_first_value_from_column("details", "detail_id"), get_last_value_from_column("details", "detail_id") + 1):
                    details_in_rooms_data = {
                        "room_id": j,
                        "detail_id": k,
                        "available": random.choice(["TRUE", "FALSE"])
                    }
                    insert_query = """
                    INSERT INTO details_in_rooms(room_id, detail_id, available)
                    VALUES (%(room_id)s, %(detail_id)s, %(available)s)
                    """
                    cursor.execute(insert_query, details_in_rooms_data)
        
def fill_services_in_rooms():
            for j in range(get_first_value_from_column("rooms", "room_id"), get_last_value_from_column("rooms", "room_id") + 1):
                for k in range(get_first_value_from_column("services", "service_id"), get_last_value_from_column("services", "service_id") + 1):
                    services_in_rooms_data = {
                        "room_id": j,
                        "service_id": k,
                        "available": random.choice(["TRUE", "FALSE"])
                    }
                    insert_query = """
                    INSERT INTO services_in_rooms(room_id, service_id, available)
                    VALUES (%(room_id)s, %(service_id)s, %(available)s)
                    """
                    cursor.execute(insert_query, services_in_rooms_data)

def fill_user_names():
    ids = get_all_values_from_table_column('staff', 'employee_id')
    for id in ids:
        update_query = f"""
        UPDATE staff SET user_name = '{fake.user_name()}' WHERE employee_id = {id[0]}; 
        """
        cursor.execute(update_query)

def fill_staff_in_rooms():
    room_ids = get_all_values_from_table_column('rooms', 'room_id')
    employee_ids = get_all_values_from_table_column('staff', 'employee_id')
    for i in range (1250):
        insert_query = f"""
        INSERT INTO staff_in_rooms (employee_id, room_id)
        VALUES ({random.choice(employee_ids)[0]}, {random.choice(room_ids)[0]})
        """
        try:
            cursor.execute(insert_query)
            conn.commit()
        except:
            continue

def fill_orders():
    for i in range(5000):
        random_room = random.choice(get_all_values_from_table_column('rooms', 'room_id'))
        random_master = random.choice(get_all_masters_from_room(random_room))
        order_data = {
            "room_id": random_room,
            "client_id": random.choice(get_all_values_from_table_column('clients', 'client_id')),
            "order_date": fake.date_between(start_date=datetime(2024, 1, 1), end_date=datetime(2024, 11, 11)),
            "state": random.choice(['Выполняется', 'Выполнен']),
            "master_id": random_master
        }
        insert_query = """
        INSERT INTO orders (room_id, client_id, order_date, state, master_id)
        VALUES(%(room_id)s, %(client_id)s, %(order_date)s, %(state)s, %(master_id)s)
        """
        cursor.execute(insert_query, order_data)

        
def main():
    connect_to_db()
    get_all_masters_id()
    close_conn()
main()