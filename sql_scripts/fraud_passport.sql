-- создаем таблицу с данными не действующих клиентов
CREATE TABLE if not exists STG_CLIENT_NOT_VALID as
    SELECT
        client_id, 
        passport_num,
        last_name || ' ' || first_name || ' ' || patronymic as fio,
        phone,
        passport_valid_to
    FROM DWH_DIM_CLIENTS
    WHERE ? > passport_valid_to 
    or passport_num in (
        SELECT 
            passport_num 
        FROM DWH_FACT_PASSPORT_BLACKLIST
        );

-- недействующие аккаунты
CREATE VIEW if not exists STG_ACCOUNT_NOT_VALID as
    SELECT
        t1.account_num,
        t2.passport_num,
        t2.fio,
        t2.phone
    FROM DWH_DIM_ACCOUNTS t1
    INNER JOIN STG_CLIENT_NOT_VALID t2
    on t1.client = t2.client_id
    WHERE t2.passport_valid_to is not null;

-- недействующие карты
CREATE VIEW if not exists STG_CARDS_NOT_VALID as
    SELECT
        t1.card_num,
        t2.passport_num,
        t2.fio,
        t2.phone
    FROM DWH_DIM_CARDS t1
    inner join STG_ACCOUNT_NOT_VALID t2
    on t1.account_num = t2.account_num;

-- поиск мошейнических паспортов
CREATE VIEW if not exists STG_PASSPORT_FRAUD_VIEW as
    SELECT
        t2.fio,
        t2.passport_num as passport,
        t2.phone,
        t1.trans_date as event_dt
    FROM DWH_FACT_TRANSACTIONS t1
    INNER JOIN STG_CARDS_NOT_VALID t2
    on t1.card_num = t2.card_num

