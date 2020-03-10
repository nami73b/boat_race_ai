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
    rp1.race_date as race_date_old1,
    rp1.place_id as place_id_old1,
    rp1.race_no as race_no_old1,
    rp1.bracket_no as bracket_no_old1,
    rp1.is_miss as is_miss_old1,
    rp1.player_id as player_id_old1,
    rp1.player_grade as player_grade_old1,
    rp1.branch as branch_old1,
    rp1.born_area as born_area_old1,
    rp1.age as age_old1,
    rp1.weight as weight_old1,
    rp1.f_count as f_count_old1,
    rp1.l_count as l_count_old1,
    rp1.start_time_avg as start_time_avg_old1,
    rp1.first_rate_all as first_rate_all_old1,
    rp1.second_rate_all as second_rate_all_old1,
    rp1.third_rate_all as third_rate_all_old1,
    rp1.first_rate_area as first_rate_area_old1,
    rp1.second_rate_area as second_rate_area_old1,
    rp1.third_rate_area as third_rate_area_old1,
    rp1.motor_no as motor_no_old1,
    rp1.motor_within_second_rate as motor_within_second_rate_old1,
    rp1.motor_within_third_rate as motor_within_third_rate_old1,
    rp1.boat_no as boat_no_old1,
    rp1.boat_within_second_rate as boat_within_second_rate_old1,
    rp1.boat_within_third_rate as boat_within_third_rate_old1,
    rp1.pre_time as pre_time_old1,
    rp1.tilt_angle as tilt_angle_old1,
    rp1.propeller as propeller_old1,
    rp1.parts as parts_old1,
    rp1.adjust_weight as adjust_weight_old1,
    rp1.pre_start_timing as pre_start_timing_old1,
    rp1.finish_order as finish_order_old1,
    rp1.player_race_time as player_race_time_old1,
    rp1.start_timing as start_timing_old1,
    rp1.win_pattern as win_pattern_old1,
    rp2.race_date as race_date_old2,
    rp2.place_id as place_id_old2,
    rp2.race_no as race_no_old2,
    rp2.bracket_no as bracket_no_old2,
    rp2.is_miss as is_miss_old2,
    rp2.player_id as player_id_old2,
    rp2.player_grade as player_grade_old2,
    rp2.branch as branch_old2,
    rp2.born_area as born_area_old2,
    rp2.age as age_old2,
    rp2.weight as weight_old2,
    rp2.f_count as f_count_old2,
    rp2.l_count as l_count_old2,
    rp2.start_time_avg as start_time_avg_old2,
    rp2.first_rate_all as first_rate_all_old2,
    rp2.second_rate_all as second_rate_all_old2,
    rp2.third_rate_all as third_rate_all_old2,
    rp2.first_rate_area as first_rate_area_old2,
    rp2.second_rate_area as second_rate_area_old2,
    rp2.third_rate_area as third_rate_area_old2,
    rp2.motor_no as motor_no_old2,
    rp2.motor_within_second_rate as motor_within_second_rate_old2,
    rp2.motor_within_third_rate as motor_within_third_rate_old2,
    rp2.boat_no as boat_no_old2,
    rp2.boat_within_second_rate as boat_within_second_rate_old2,
    rp2.boat_within_third_rate as boat_within_third_rate_old2,
    rp2.pre_time as pre_time_old2,
    rp2.tilt_angle as tilt_angle_old2,
    rp2.propeller as propeller_old2,
    rp2.parts as parts_old2,
    rp2.adjust_weight as adjust_weight_old2,
    rp2.pre_start_timing as pre_start_timing_old2,
    rp2.finish_order as finish_order_old2,
    rp2.player_race_time as player_race_time_old2,
    rp2.start_timing as start_timing_old2,
    rp2.win_pattern as win_pattern_old2,
    rp3.race_date as race_date_old3,
    rp3.place_id as place_id_old3,
    rp3.race_no as race_no_old3,
    rp3.bracket_no as bracket_no_old3,
    rp3.is_miss as is_miss_old3,
    rp3.player_id as player_id_old3,
    rp3.player_grade as player_grade_old3,
    rp3.branch as branch_old3,
    rp3.born_area as born_area_old3,
    rp3.age as age_old3,
    rp3.weight as weight_old3,
    rp3.f_count as f_count_old3,
    rp3.l_count as l_count_old3,
    rp3.start_time_avg as start_time_avg_old3,
    rp3.first_rate_all as first_rate_all_old3,
    rp3.second_rate_all as second_rate_all_old3,
    rp3.third_rate_all as third_rate_all_old3,
    rp3.first_rate_area as first_rate_area_old3,
    rp3.second_rate_area as second_rate_area_old3,
    rp3.third_rate_area as third_rate_area_old3,
    rp3.motor_no as motor_no_old3,
    rp3.motor_within_second_rate as motor_within_second_rate_old3,
    rp3.motor_within_third_rate as motor_within_third_rate_old3,
    rp3.boat_no as boat_no_old3,
    rp3.boat_within_second_rate as boat_within_second_rate_old3,
    rp3.boat_within_third_rate as boat_within_third_rate_old3,
    rp3.pre_time as pre_time_old3,
    rp3.tilt_angle as tilt_angle_old3,
    rp3.propeller as propeller_old3,
    rp3.parts as parts_old3,
    rp3.adjust_weight as adjust_weight_old3,
    rp3.pre_start_timing as pre_start_timing_old3,
    rp3.finish_order as finish_order_old3,
    rp3.player_race_time as player_race_time_old3,
    rp3.start_timing as start_timing_old3,
    rp3.win_pattern as win_pattern_old3,
    rd1.race_grade as race_grade_old1,
    rd1.distance as distance_old1,
    rd1.course_direction as course_direction_old1,
    rd1.weather as weather_old1,
    rd1.temperature as temperature_old1,
    rd1.wind as wind_old1,
    rd1.wind_direction as wind_direction_old1,
    rd1.water_temperature as water_temperature_old1,
    rd1.wave_height as wave_height_old1,
    rd2.race_grade as race_grade_old2,
    rd2.distance as distance_old2,
    rd2.course_direction as course_direction_old2,
    rd2.weather as weather_old2,
    rd2.temperature as temperature_old2,
    rd2.wind as wind_old2,
    rd2.wind_direction as wind_direction_old2,
    rd2.water_temperature as water_temperature_old2,
    rd2.wave_height as wave_height_old2,
    rd3.race_grade as race_grade_old3,
    rd3.distance as distance_old3,
    rd3.course_direction as course_direction_old3,
    rd3.weather as weather_old3,
    rd3.temperature as temperature_old3,
    rd3.wind as wind_old3,
    rd3.wind_direction as wind_direction_old3,
    rd3.water_temperature as water_temperature_old3,
    rd3.wave_height as wave_height_old3

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
                                    and rpt1.is_miss = 'False'
                                order by rpt1.race_date_no desc limit 1
                            )

left join race_player rp2
    on rp.player_id = rp2.player_id
    and rp2.race_date_no = (select race_date_no from race_player rpt2
                                where rpt2.race_date_no < rp1.race_date_no
                                    and rp.player_id = rpt2.player_id
                                    and rpt2.is_miss = 'False'
                                order by rpt2.race_date_no desc limit 1
                            )

left join race_player rp3
    on rp.player_id = rp3.player_id
    and rp3.race_date_no = (select race_date_no from race_player rpt3
                                where rpt3.race_date_no < rp2.race_date_no
                                    and rp.player_id = rpt3.player_id
                                    and rpt3.is_miss = 'False'
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