create table race_detail (race_date varchar(10), place_id varchar(10), race_no varchar(4),race_name varchar(100),race_grade varchar(100), race_class varchar(100), distance int,course_direction varchar(10),weather varchar(4),temperature float, wind float,wind_direction varchar(4),water_temperature float ,wave_height float);

alter table race_detail add primary key(race_date, place_id, race_no);