DROP TABLE if exists DWH_FACT_PASSPORT_BLACKLIST;
CREATE TABLE if not exists DWH_FACT_PASSPORT_BLACKLIST(
    passport_num varchar(128),
    entry_dt date
);