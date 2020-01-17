select
    rp.race_date,
    rp.place_id,
    rp.race_no,
    rp.race_date_no,
    rp.bracket_no,
    rp.is_miss,
    rp.player_id,
    rp.player_grade,
    rp.branch,
    rp.born_area,
    rp.age,
    rp.weight,
    rp.f_count,
    rp.l_count,
    rp.start_time_avg,
    rp.first_rate_all,
    rp.second_rate_all,
    rp.third_rate_all,
    rp.first_rate_area,
    rp.second_rate_area,
    rp.third_rate_area,
    rp.motor_no,
    rp.motor_within_second_rate,
    rp.motor_within_third_rate,
    rp.boat_no,
    rp.boat_within_second_rate,
    rp.boat_within_third_rate,
    rp.pre_time,
    rp.tilt_angle,
    rp.propeller,
    rp.parts,
    rp.adjust_weight,
    rp.pre_start_timing,
    rp.finish_order,
    rp.player_race_time,
    rp.start_timing,
    rp.win_pattern,
    rd.race_grade,
    rd.distance,
    rd.course_direction,
    rd.weather,
    rd.temperature,
    rd.wind,
    rd.wind_direction,
    rd.water_temperature,
    rd.wave_height,
    rp1.race_date,
    rp1.place_id,
    rp1.race_no,
    rp1.bracket_no,
    rp1.is_miss,
    rp1.player_id,
    rp1.player_grade,
    rp1.branch,
    rp1.born_area,
    rp1.age,
    rp1.weight,
    rp1.f_count,
    rp1.l_count,
    rp1.start_time_avg,
    rp1.first_rate_all,
    rp1.second_rate_all,
    rp1.third_rate_all,
    rp1.first_rate_area,
    rp1.second_rate_area,
    rp1.third_rate_area,
    rp1.motor_no,
    rp1.motor_within_second_rate,
    rp1.motor_within_third_rate,
    rp1.boat_no,
    rp1.boat_within_second_rate,
    rp1.boat_within_third_rate,
    rp1.pre_time,
    rp1.tilt_angle,
    rp1.propeller,
    rp1.parts,
    rp1.adjust_weight,
    rp1.pre_start_timing,
    rp1.finish_order,
    rp1.player_race_time,
    rp1.start_timing,
    rp1.win_pattern,
    rp2.race_date,
    rp2.place_id,
    rp2.race_no,
    rp2.bracket_no,
    rp2.is_miss,
    rp2.player_id,
    rp2.player_grade,
    rp2.branch,
    rp2.born_area,
    rp2.age,
    rp2.weight,
    rp2.f_count,
    rp2.l_count,
    rp2.start_time_avg,
    rp2.first_rate_all,
    rp2.second_rate_all,
    rp2.third_rate_all,
    rp2.first_rate_area,
    rp2.second_rate_area,
    rp2.third_rate_area,
    rp2.motor_no,
    rp2.motor_within_second_rate,
    rp2.motor_within_third_rate,
    rp2.boat_no,
    rp2.boat_within_second_rate,
    rp2.boat_within_third_rate,
    rp2.pre_time,
    rp2.tilt_angle,
    rp2.propeller,
    rp2.parts,
    rp2.adjust_weight,
    rp2.pre_start_timing,
    rp2.finish_order,
    rp2.player_race_time,
    rp2.start_timing,
    rp2.win_pattern,
    rp3.race_date,
    rp3.place_id,
    rp3.race_no,
    rp3.bracket_no,
    rp3.is_miss,
    rp3.player_id,
    rp3.player_grade,
    rp3.branch,
    rp3.born_area,
    rp3.age,
    rp3.weight,
    rp3.f_count,
    rp3.l_count,
    rp3.start_time_avg,
    rp3.first_rate_all,
    rp3.second_rate_all,
    rp3.third_rate_all,
    rp3.first_rate_area,
    rp3.second_rate_area,
    rp3.third_rate_area,
    rp3.motor_no,
    rp3.motor_within_second_rate,
    rp3.motor_within_third_rate,
    rp3.boat_no,
    rp3.boat_within_second_rate,
    rp3.boat_within_third_rate,
    rp3.pre_time,
    rp3.tilt_angle,
    rp3.propeller,
    rp3.parts,
    rp3.adjust_weight,
    rp3.pre_start_timing,
    rp3.finish_order,
    rp3.player_race_time,
    rp3.start_timing,
    rp3.win_pattern,
    rd1.race_grade,
    rd1.distance,
    rd1.course_direction,
    rd1.weather,
    rd1.temperature,
    rd1.wind,
    rd1.wind_direction,
    rd1.water_temperature,
    rd1.wave_height,
    rd2.race_grade,
    rd2.distance,
    rd2.course_direction,
    rd2.weather,
    rd2.temperature,
    rd2.wind,
    rd2.wind_direction,
    rd2.water_temperature,
    rd2.wave_height,
    rd3.race_grade,
    rd3.distance,
    rd3.course_direction,
    rd3.weather,
    rd3.temperature,
    rd3.wind,
    rd3.wind_direction,
    rd3.water_temperature,
    rd3.wave_height

from (select * from race_player where race_date = '20191111') rp
inner join race_detail rd
    on rd.race_date = rp.race_date
    and rd.place_id = rp.place_id
    and rd.race_no = rp.race_no

left join race_player rp1
    on rp.player_id = rp1.player_id
    and rp1.race_date_no = (select race_date_no from race_player rpt1
                                where rpt1.race_date_no < rp.race_date_no
                                    and rp.player_id = rpt1.player_id
                                order by rpt1.race_date_no desc limit 1
                            )

left join race_player rp2
    on rp.player_id = rp2.player_id
    and rp2.race_date_no = (select race_date_no from race_player rpt2
                                where rpt2.race_date_no < rp1.race_date_no
                                    and rp.player_id = rpt2.player_id
                                order by rpt2.race_date_no desc limit 1
                            )

left join race_player rp3
    on rp.player_id = rp3.player_id
    and rp3.race_date_no = (select race_date_no from race_player rpt3
                                where rpt3.race_date_no < rp2.race_date_no
                                    and rp.player_id = rpt3.player_id
                                order by rpt3.race_date_no desc limit 1
                            )
left join race_detail rd1
    on rd1.race_date = rp1.race_date
    and rd1.place_id = rp1.place_id
    and rd1.race_no = rp1.race_no

left join race_detail rd2
    on rd2.race_date = rp2.race_date
    and rd2.place_id = rp2.place_id
    and rd2.race_no = rp2.race_no

left join race_detail rd3
    on rd3.race_date = rp3.race_date
    and rd3.place_id = rp3.place_id
    and rd3.race_no = rp3.race_no