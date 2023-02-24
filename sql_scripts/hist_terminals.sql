create table if not exists DWH_DIM_TERMINALS_HIST(
    id integer primary key autoincrement,
    terminal_id varchar(128),
    terminal_type varchar(128),
    terminal_city varchar(128),
    terminal_address varchar(128),
    effective_from date default current_timestamp,
    effective_to date default (datetime('2999-12-31 23:59:59')),
    deleted_flg default 0
);
CREATE VIEW if not exists STG_TERMINALS_VIEW as
    SELECT
        terminal_id, terminal_type, terminal_city, terminal_address
    FROM DWH_DIM_TERMINALS_HIST
    WHERE current_timestamp BETWEEN effective_from and effective_to;

-- временная таблица для новых терминалов

CREATE TABLE if not exists STG_TERMINALS_NEW as
    SELECT
        t1.*
    FROM STG_TERMINALS as t1
    LEFT JOIN STG_TERMINALS_VIEW as t2
    on t1.terminal_id == t2.terminal_id
    WHERE t2.terminal_id is null;

-- временная таблица для недействующих терминалов

CREATE TABLE if not exists STG_TERMINALS_DELETE as
    SELECT
        t1.*
    FROM STG_TERMINALS_VIEW as t1
    LEFT JOIN STG_TERMINALS as t2
    on t1.terminal_id == t2.terminal_id
    WHERE t2.terminal_id is null;

-- временная таблица для измененных терминалов

create table if not exists STG_TERMINALS_CHANGED as
    SELECT
        t1.*
    FROM STG_TERMINALS as t1
    INNER JOIN STG_TERMINALS_VIEW as t2
    on t1.terminal_id == t2.terminal_id
    and
        (
            t1.terminal_address <> t2.terminal_address or
            t1.terminal_type <> t2.terminal_type or
            t1.terminal_city <> t2.terminal_city
        );

-- обновление таблицы удаленные записи (втавляем данные в DWH_DIM_TERMINALS_HIST)

UPDATE DWH_DIM_TERMINALS_HIST
    SET effective_to = datetime('now', '-1 second'),
    deleted_flg = 1
    WHERE terminal_id in (
        SELECT 
            terminal_id 
        FROM STG_TERMINALS_DELETE
        )
    and effective_to = datetime('2999-12-31 23:59:59');

-- обновляем новые записи 

INSERT INTO DWH_DIM_TERMINALS_HIST(
    terminal_id,
    terminal_city,
    terminal_type,
    terminal_address
    ) 
SELECT
    terminal_id,
    terminal_city,
    terminal_type,
    terminal_address
FROM STG_TERMINALS_NEW;

-- обновляем и вставляем измененные данные

UPDATE DWH_DIM_TERMINALS_HIST
    SET effective_to = datetime('now', '-1 second')
    WHERE terminal_id in (select terminal_id from STG_TERMINALS_CHANGED)
    and effective_to = datetime('2999-12-31 23:59:59');

INSERT INTO DWH_DIM_TERMINALS_HIST(
    terminal_id,
    terminal_city,
    terminal_type,
    terminal_address
    ) 
SELECT
    terminal_id,
    terminal_city,
    terminal_type,
    terminal_address
FROM STG_TERMINALS_CHANGED;

DROP table if exists STG_TERMINALS;
DROP table if exists STG_TERMINALS_NEW;
DROP table if exists STG_TERMINALS_DELETE;
DROP table if exists STG_TERMINALS_CHANGED;
DROP view if exists STG_TERMINALS_VIEW;

-- Отправить файл в бэкап
-- source_file = "terminals_" + date + ".xlsx"
-- backup_file = os.path.join("archive", "terminals_" + date + ".xlsx.backup")
-- os.rename(source_file, backup_file)

