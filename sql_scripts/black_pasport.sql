DROP TABLE if exists STG_PASSPORT_BLACKLIST;
CREATE TABLE if not exists STG_PASSPORT_BLACKLIST(
    entry_dt timestamp,
    passport_num varchar(128) primary key,
    foreign key (passport_num) references DWH_DIM_CLIENTS (passport_num)
);
