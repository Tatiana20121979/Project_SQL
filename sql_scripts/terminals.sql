DROP TABLE if exists STG_TERMINALS;
CREATE TABLE if not exists STG_TERMINALS(
    terminal_id varchar(128) primary key,
    terminal_type varchar(128),
    terminal_city varchar(128),
    terminal_address varchar(500),
    foreign key (terminal_id) references STG_TRANSACTIONS (terminal)
);