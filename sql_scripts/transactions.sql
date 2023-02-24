DROP TABLE if exists STG_TRANSACTIONS;
CREATE TABLE if not exists STG_TRANSACTIONS (
    trans_id varchar(128),
    trans_date date,
    amt decimal(10,2),
    card_num varchar(128),
    oper_type varchar(128),
    oper_result varchar(128),
    terminal varchar(128),
    foreign key (card_num) references STG_CARDS (card_num),
    foreign key (terminal) references STG_TERMINALS (terminal_id)
)