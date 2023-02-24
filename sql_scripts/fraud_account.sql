CREATE TABLE if not exists STG_ACCOUNT_NOT_VALID as
    SELECT
        t1.account_num, 
        t2.passport_num, 
        t2.phone,
        t2.last_name || ' ' || t2.first_name || ' ' || t2.patronymic as fio,
        t1.valid_to
    FROM DWH_DIM_ACCOUNTS t1
    LEFT JOIN DWH_DIM_CLIENTS t2 on t1.client = t2.client_id
    WHERE ? > t1.valid_to;


CREATE VIEW if not exists STG_CARDS_NOT_VALID as
    SELECT
        t1.card_num, 
        t2.passport_num, 
        t2.fio, 
        t2.phone
    FROM DWH_DIM_CARDS t1
    INNER JOIN STG_ACCOUNT_NOT_VALID t2 on t1.account_num=t2.account_num;

CREATE VIEW if not exists STG_PASSPORT_FRAUD_VIEW as
    SELECT
        t2.fio, 
        t2.passport_num as passport, 
        t2.phone,
        t1.trans_date as event_dt
    FROM DWH_FACT_TRANSACTIONS t1
    INNER JOIN STG_CARDS_NOT_VALID t2 on t1.card_num = t2.card_num;



-- вставка данных в витрину
INSERT INTO REP_FRAUD (
    event_dt,
    passport,
    fio,
    phone,
    event_type,
    report_dt
) SELECT
    event_dt,
    passport,
    fio,
    phone,
    'account',
    datetime('now')
FROM STG_PASSPORT_FRAUD_VIEW
WHERE (event_dt, passport) in (
    SELECT 
        MIN(event_dt), 
        passport
    FROM STG_PASSPORT_FRAUD_VIEW
    GROUP BY passport
);


drop table if exists STG_ACCOUNT_NOT_VALID;
drop view if exists STG_CARDS_NOT_VALID;
drop view if exists STG_PASSPORT_FRAUD_VIEW;



