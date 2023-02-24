CREATE TABLE if not exists REP_FRAUD(
    event_dt date,
    passport varchar(128),
    fio varchar(364),
    phone varchar(128),
    event_type varchar(128),
    report_dt date default current_timestamp
);