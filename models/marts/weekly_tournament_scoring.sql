with match_placements as (
select
  tournament_week_key
  , game_title_key
  , team_number
  , match_id
  , min(placement) as team_placement
  , sum(kills) as kills
  , sum(damage_done) as damage_dealt
  , sum(damage_taken) as damage_taken

from {{ ref('tournament_gameplay') }}

group by 1,2,3,4
)

, game_match_scores as (
select
  mp.tournament_week_key
  , mp.game_title_key
  , mp.team_placement
  , mp.team_number
  , case
      when mp.team_placement = 1 then first_place
      when mp.team_placement = 2 then second_place
      when mp.team_placement = 3 then third_place
      when mp.team_placement = 4 then fourth_place
      when mp.team_placement = 5 then fifth_place
      when mp.team_placement <= 10 then top_ten
      when mp.team_placement <= 15 then top_fifteen
      when mp.team_placement <= 25 then top_twenty_five
      else 0
    end as points
   , mp.kills * tr.points_per_kill as kill_points
   , mp.damage_dealt
   , mp.damage_taken

from match_placements as mp
left join {{ ref('tournament_rules') }} as tr
  on mp.tournament_week_key = tr.tournament_week_key

where mp.tournament_week_key is not null
)

, row_number_games as (
select
  gms.tournament_week_key
  , gms.game_title_key
  , gms.team_placement
  , gms.team_number
  , gms.points
  , gms.kill_points
  , gms.damage_dealt
  , gms.damage_taken
  , row_number() over (partition by gms.tournament_week_key, gms.game_title_key, gms.team_number order by (gms.points + gms.kill_points) desc) as game_rank

from game_match_scores as gms

)

, top_five_games as (
select
  *
from row_number_games
where game_rank <= 5
)

select
  tfg.tournament_week_key
  , tfg.game_title_key
  , tfg.team_number
  , tt.captain_athlete_user_key
  , sum(tfg.damage_dealt) as damage_dealt
  , sum(tfg.damage_taken) as damage_taken
  , sum(tfg.points) as placement_score
  , sum(tfg.kill_points) as kill_points
  , sum(tfg.points) + sum(tfg.kill_points)  as total_score

from top_five_games as tfg
left join {{ ref('tournament_teams') }}  as tt
  on tfg.tournament_week_key = tt.tournament_week_key
  and tfg.game_title_key = tt.game_title_key
  and tfg.team_number = tt.team_number

group by 1,2,3,4
order by total_score desc
