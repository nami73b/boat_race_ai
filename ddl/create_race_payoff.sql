create table race_payoff (race_date varchar(10), place_id varchar(10), race_no varchar(4), bet_type int, sub_number int , bracket1 int, bracket2 int , bracket3 int, payoff int);

alter table race_payoff add primary key(race_date, place_id, race_no, bet_type, sub_number);
