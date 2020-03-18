create table race_detail (race_date varchar(10), place_id varchar(10), race_no varchar(4), bet_type int, sub_number int, bracket_no_1 varchar(10), bracket_no_2 varchar(10), bracket_no_3 varchar(10), amount int, odds float);

alter table race_detail add primary key(race_date, place_id, race_no, bet_type, sub_number);
