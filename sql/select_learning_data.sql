select
    rp.race_date,
    rp.place_id,
    rp.race_no,
    rp.bracket_no,
    rp.is_miss,
    rp.place_id,
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
    rd.wace_height
from race_player rp
inner join race_detail rd
    on rd.race_date = rp.race_date
    and rd.place_id = rp.place_id
    and rd.race_no = rp.race_no