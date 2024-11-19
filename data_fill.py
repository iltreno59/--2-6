from faker import Faker
import psycopg2

def adapt_phone_number(phone_number):
    phone_number = ''.join(filter(str.isdigit, phone_number))
    phone_number = phone_number.replace(phone_number[0], '8', 1)
    return phone_number


fake = Faker('ru_RU')

conn_params = {
    "dbname": "moto_and_cars",
    "user": "postgres",
    "password": "nikita",
    "host": "localhost",
    "port": "5432" 
}
try:
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
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

except psycopg2.Error as e:
    print("Ошибка при подключении к базе данных:", e)
    
finally:
    conn.commit()
    if cursor:
        cursor.close()
    if conn:
        conn.close()
        print("Соединение с базой данных закрыто.")