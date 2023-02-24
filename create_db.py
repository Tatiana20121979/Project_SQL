import sqlite3
import os
import pandas as pd
import re



def init_db (db_filename, db_script_filename):
    with sqlite3.connect(db_filename) as conn: 
        cursor = conn.cursor()
        with open(db_script_filename, 'r', encoding='UTF8') as f:
            db_script = f.read()
            cursor.executescript(db_script)
            conn.commit()

# db_filename = 'database.db'

# функция загрузки данных в таблицу транзакций
def load_transations_file(db_filename, transactions_filename):
    colnames = ['trans_id', 'trans_date', 'amt', 'card_num', 'oper_type', 'oper_result', 'terminal']
    data = pd.read_csv(transactions_filename, names=colnames, header=0, sep =';', dtype={'transaction_id': str})
    with sqlite3.connect(db_filename) as conn:  
        data.to_sql('STG_TRANSACTIONS', conn, if_exists='replace', index=False)
    

# функция загрузки данных в таблицу терминалов
def load_terminals_file(db_filename, terminals_filename):
    colnames = ['terminal_id', 'terminal_type', 'terminal_city', 'terminal_address'] 
    data = pd.read_excel(terminals_filename, names=colnames, header = 0)
    with sqlite3.connect(db_filename) as conn:  
        data.to_sql('STG_TERMINALS', conn, if_exists='replace', index=False)

# функция загрузки данных в таблицу чёрных паспартов
def load_black_passport_file(db_filename, terminals_filename):
    colnames = ['entry_dt', 'passport_num'] 
    data = pd.read_excel(terminals_filename, names=colnames, header = 0)
    with sqlite3.connect(db_filename) as conn:  
        data.to_sql('STG_PASSPORT_BLACKLIST', conn, if_exists='replace', index=False)

# функция загрузки в базу данных исходные данные
def init_load_data(db_filename):
    init_db(db_filename, "sql_scripts\init.sql")
    # создаем таблицу с транзакциями считывая файл transations.sql
    init_db(db_filename, r"sql_scripts\transactions.sql")
    # создаем таблицу с транзакциями считывая файл terminals.sql
    init_db(db_filename, r"sql_scripts\terminals.sql")
    # создаем таблицу с транзакциями считывая файл black_pasport.sql
    init_db(db_filename, r"sql_scripts\black_pasport.sql")
    for root, dirs, files in os.walk("./data", topdown=False):
        for name in files:
            print(name)
            if name.endswith('.txt'):
                load_transations_file(db_filename, root + '/' + name)
            elif name.startswith('terminals_'):
                load_terminals_file(db_filename, root + '/' + name)
            elif name.startswith('passport_blacklist_'):
                load_black_passport_file(db_filename, root + '/' + name)

# инициализация базы данных исходными данными
init_load_data('database.db')
# # инициализация таблицы фактов транзакций (накопительным итогом, не меняется)
# init_db('database.db', r"sql_scripts\fact_transactions.sql")
# # инициализация таблицы фактов чёрных паспортов (накопительным итогом, не меняется)
# init_db('database.db', r"sql_scripts\fact_black_pasport.sql")
# # инициализация таблицы витрины данных
# init_db('database.db', r"sql_scripts\fraud.sql")

# def get_date():
#     date = ''
#     lst = os.listdir('./data')
#     lst.sort()
#     for fname in lst:
#         if fname.startswith('transactions'):
#             date = fname.split("_",1)[1].split(".",1)[0]
#             break
#         if date == '':
#             raise Exception ('Файл не найден')
#         if not os.path.isfile('transactions_' + date + '.txt'):
#             raise Exception ('Файл c транзакциями не найден')
#         return date

# --------------------------------------------------------------------------------------------
# СОЗДАЕМ ТАБЛИЦУ ВИТРИНЫ ДАННЫХ
def create_rep_fraud_table():
    with sqlite3.connect('database.db') as conn: 
        cursor = conn.cursor()
    cursor.executescript('''
        CREATE TABLE if not exists REP_FRAUD(
            event_dt date,
            passport varchar(128),
            fio varchar(364),
            phone varchar(128),
            event_type varchar(128),
            report_dt date default current_timestamp
        );
    ''')
    conn.commit()

create_rep_fraud_table()
# --------------------------------------------------------------------------------------------
# СОЗДАЕМ ТАБЛИЦУ ФАКТОВ ТРАНЗАКЦИЙ (ДЛЯ ЗАГРУЗКИ НАКОПИТЕЛЬНЫМ ИТОГОМ ИЗ ТАБЛИЦЫ STG_TRANSACTIONS)
def create_transactions_fact_table():
    with sqlite3.connect('database.db') as conn: 
        cursor = conn.cursor()
    cursor.executescript('''
        CREATE TABLE if not exists DWH_FACT_TRANSACTIONS(
            trans_id varchar(128),
            trans_date date,
            amt decimal(10,2),
            card_num varchar(128),
            oper_type varchar(128),
            oper_result varchar(128),
            terminal varchar(128),
            foreign key (card_num) references DWH_DIM_CARDS (card_num),
            foreign key (terminal) references DWH_DIM_TERMINALS_HIST (terminal_id)
        );
    ''')
    conn.commit()

create_transactions_fact_table()

# создаем функцию выгрузки данных по транзакциям из временной таблицы (за день) STG_TRANSACTIONS 
# в таблицу фактов DWH_FACT_TRANSACTIONS

def transactions_to_fact():
    with sqlite3.connect('database.db') as conn: 
        cursor = conn.cursor()
    cursor.executescript('''
        INSERT INTO DWH_FACT_TRANSACTIONS(
            trans_id, 
            trans_date, 
            amt, 
            card_num, 
            oper_type, 
            oper_result, 
            terminal
        ) 
        SELECT 
            trans_id, 
            trans_date, 
            amt, 
            card_num, 
            oper_type, 
            oper_result, 
            terminal
        FROM STG_TRANSACTIONS;
    ''')
    # cursor.execute('''
    #     DROP TABLE if exists STG_TRANSACTIONS; 
    # ''')
    conn.commit()
    # тут добжен быть бэкап файла примерно как то так
    # backup_file = os.path.join("archive", "transactions_" + date + ".txt.backup")
    # os.rename(source_file, backup_file)
    # return data

transactions_to_fact()
# как сделать что б не задвоить выгрузку?

# СОЗДАЕМ ТАБЛИЦУ ФАКТОВ ТРАНЗАКЦИЙ (ДЛЯ ЗАГРУЗКИ НАКОПИТЕЛЬНЫМ ИТОГОМ ИЗ ТАБЛИЦЫ STG_PASSPORT_BLACKLIST)
def create_black_pasport_fact_table():
    with sqlite3.connect('database.db') as conn: 
        cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE if not exists DWH_FACT_PASSPORT_BLACKLIST(
            entry_dt timestamp,
            passport_num varchar(128)           
       );
    ''')
    conn.commit()

def black_pasport_to_fact():
    with sqlite3.connect('database.db') as conn: 
        cursor = conn.cursor()
    cursor.executescript('''
        INSERT INTO DWH_FACT_PASSPORT_BLACKLIST(
            entry_dt, 
            passport_num
        ) 
        SELECT 
            entry_dt, 
            passport_num
        FROM STG_PASSPORT_BLACKLIST;
    ''')
create_black_pasport_fact_table()
black_pasport_to_fact()

# создаем функцию выгрузки данных по транзакциям из временной таблицы (за день) STG_PASSPORT_BLACKLIST 
# в таблицу фактов DWH_FACT_PASSPORT_BLACKLIST



# def black_pasport_to_fact():
#     with sqlite3.connect('database.db') as conn: 
#         cursor = conn.cursor()
#     cursor.executescript('''
#         INSERT INTO DWH_FACT_TRANSACTIONS(
#            entry1_dt,
#            passport_num
#         ) 
#         VALUES('2020-10-10 00:00:00', '123'   
#         )
#     ''')
    # cursor.execute('''
    #     DROP TABLE if exists STG_PASSPORT_BLACKLIST; 
    # ''')
    # conn.commit()
    # тут добжен быть бэкап файла примерно как то так
    # backup_file = os.path.join("archive", "transactions_" + date + ".txt.backup")
    # os.rename(source_file, backup_file)
    # return data

# black_pasport_to_fact()
# qlite3.OperationalError: table DWH_FACT_TRANSACTIONS has no column named entry_dt
# ----------------------------------------------------------------------------------------
# CОЗДАЕМ ТАБЛИЦУ И ПРЕДСТАВЛЕНИЕ - ИСТОРИЧЕСКУЮ С ТЕРМИНАЛАМИ (ИНКРЕМЕНТАЛЬНАЯ ЗАГРУЗКА)
# def terminals_hist_table():
#     with sqlite3.connect('database.db') as conn: 
#         cursor = conn.cursor()
#     cursor.executescript('''
#         CREATE TABLE if not exists DWH_DIM_TERMINALS_HIST(
#             id integer primary key autoincrement,
#             terminal_id varchar(128),
#             terminal_type varchar(128),
#             terminal_city varchar(128),
#             terminal_address varchar(128),
#             effective_from date default current_timestamp,
#             effective_to date default (datetime('2999-12-31 23:59:59')),
#             deleted_flg default 0
#         );
#         CREATE VIEW if not exists STG_TERMINALS_VIEW as
#         SELECT
#             terminal_id, terminal_type, terminal_city, terminal_address
#         FROM DWH_DIM_TERMINALS_HIST
#         WHERE current_timestamp BETWEEN effective_from and effective_to;
#     ''')


# # Создаем временную таблицу для новых терминалов
# def create_new_rows_table():
#     with sqlite3.connect('database.db') as conn: 
#         cursor = conn.cursor()
#     cursor.executescript('''
#         CREATE TABLE if not exists STG_TERMINALS_NEW as
#             SELECT
#                 t1.*
#             FROM STG_TERMINALS as t1
#             LEFT JOIN STG_TERMINALS_VIEW as t2
#             on t1.terminal_id == t2.terminal_id
#             WHERE t2.terminal_id is null;
#     ''')

# # Создаем временную таблицу для недействующих терминалов

# def create_del_rows_table():
#     with sqlite3.connect('database.db') as conn: 
#         cursor = conn.cursor()
#     cursor.executescript('''
#         CREATE TABLE if not exists STG_TERMINALS_DELETE as
#             SELECT
#                 t1.*
#             FROM STG_TERMINALS_VIEW as t1
#             LEFT JOIN STG_TERMINALS as t2
#             on t1.terminal_id == t2.terminal_id
#             WHERE t2.terminal_id is null;
#     ''')


# # Создаем временную таблицу для измененных терминалов
# def create_change_rows_table():
#     with sqlite3.connect('database.db') as conn: 
#         cursor = conn.cursor()
#     cursor.executescript('''
#         CREATE TABLE if not exists STG_TERMINALS_CHANGED as
#             SELECT
#                 t1.*
#             FROM STG_TERMINALS as t1
#             INNER JOIN STG_TERMINALS_VIEW as t2
#             on t1.terminal_id == t2.terminal_id
#             and
#                 (
#                     t1.terminal_address <> t2.terminal_address or
#                     t1.terminal_type <> t2.terminal_type or
#                     t1.terminal_city <> t2.terminal_city
#                 );
#     ''')

# # Создаем временную таблицу обновления удаленные записи (втавляем данные в DWH_DIM_TERMINALS_HIST)
# def update_hist_table():
#     with sqlite3.connect('database.db') as conn: 
#         cursor = conn.cursor()
#     cursor.executescript('''
#         UPDATE DWH_DIM_TERMINALS_HIST
#             SET effective_to = datetime('now', '-1 second'),
#             deleted_flg = 1
#             WHERE terminal_id in (
#                 SELECT 
#                     terminal_id 
#                 FROM STG_TERMINALS_DELETE
#                 )
#             and effective_to = datetime('2999-12-31 23:59:59');
#         INSERT INTO DWH_DIM_TERMINALS_HIST(
#             terminal_id,
#             terminal_city,
#             terminal_type,
#             terminal_address
#             ) 
#         SELECT
#             terminal_id,
#             terminal_city,
#             terminal_type,
#             terminal_address
#         FROM STG_TERMINALS_NEW;
#         UPDATE DWH_DIM_TERMINALS_HIST
#             SET effective_to = datetime('now', '-1 second')
#             WHERE terminal_id in (select terminal_id from STG_TERMINALS_CHANGED)
#             and effective_to = datetime('2999-12-31 23:59:59');
#         INSERT INTO DWH_DIM_TERMINALS_HIST(
#             terminal_id,
#             terminal_city,
#             terminal_type,
#             terminal_address
#             ) 
#         SELECT
#             terminal_id,
#             terminal_city,
#             terminal_type,
#             terminal_address
#         FROM STG_TERMINALS_CHANGED;
#     ''')
# def delete_tmp_table():
#     with sqlite3.connect('database.db') as conn: 
#         cursor = conn.cursor()
#     cursor.executescript('''
#         DROP table if exists STG_TERMINALS;
#         DROP table if exists STG_TERMINALS_NEW;
#         DROP table if exists STG_TERMINALS_DELETE;
#         DROP table if exists STG_TERMINALS_CHANGED;
#         DROP view if exists STG_TERMINALS_VIEW;
#     ''')


