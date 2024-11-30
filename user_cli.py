import conn_options
import psycopg2
from data_fill import adapt_phone_number

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
    if cursor:
        cursor.close()
    if conn:
        conn.close()
        print("Соединение с базой данных закрыто.")

def search_phone_number(user_phone_number):
    search_query = f"""
    SELECT EXISTS (SELECT 1 FROM clients WHERE telephone = '{user_phone_number}')
    """
    cursor.execute(search_query)
    for row in cursor:
        value = row[0]
    return value

def fill_client_data(user_phone_number):
    print("К СОЖАЛЕНИЮ, НЕ НАШЛИ ВАС В СИСТЕМЕ\n")
    user_fio = input("ВВЕДИТЕ ВАШЕ ФИО: ")
    user_mail = input("ВВЕДИТЕ ВАШУ ПОЧТУ: ")
    client_data = {
            "name": user_fio,
            "mail": user_mail,
            "telephone": user_phone_number
        }
    insert_query = """
    INSERT INTO clients(client_name, mail, telephone)
    VALUES
    (%(name)s, %(mail)s, %(telephone)s)
    """
    cursor.execute(insert_query, client_data)
    conn.commit()

def get_client_data(user_phone_number):
    values = []
    select_query = f"""
    SELECT * FROM clients WHERE telephone = '{user_phone_number}'
    """
    cursor.execute(select_query)
    for row in cursor:
        values.append(row)
    client_data = {'id': values[0][0], 'name': values[0][1], 'mail': values[0][2], 'status': values[0][3], 'bonuses': values[0][4], 'phone_number': values[0][5]}
    return client_data

def main():
    connect_to_db()

    print("""
    -----------------------------\n
    НАЧАЛО COMMAND LINE INTERFACE
    -----------------------------\n
    """)
    while True:
        client_telephone = input("ЗДРАВСТВУЙТЕ! ДЛЯ НАЧАЛА РАБОТЫ ВВЕДИТЕ ВАШ НОМЕР ТЕЛЕФОНА: ")
        client_telephone = adapt_phone_number(client_telephone)
        if len(client_telephone) == 11:
            break
        else:
            print("НЕККОРЕКТНЫЙ ФОРМАТ, ПОПРОБУЙТЕ ЕЩЁ РАЗ")
    if not search_phone_number(client_telephone):
        fill_client_data(client_telephone)
    client_data = get_client_data(client_telephone)
    print(f"ЗДРАВСТВУЙТЕ, {' '.join(client_data['name'].upper().split(' ')[1::])}. ДАВАЙТЕ Я ПОМОГУ ВАМ ОФОРМИТЬ ВАШ ЗАКАЗ")
    

    close_conn()
main()
