with match_placements as (
select
  tournament_week_key
  , game_title_key
  , team_number
  , match_id
  , athlete_user_key
  , min(placement) as team_placement
  , sum(kills) as kills
  , sum(damage_done) as damage_dealt
  , sum(damage_taken) as damage_taken

from {{ ref('tournament_gameplay') }}

group by 1,2,3,4,5
)

, game_match_scores as (
select
  mp.tournament_week_key
  , mp.game_title_key
  , mp.team_placement
  , mp.team_number
  , mp.athlete_user_key
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
  , gms.athlete_user_key
  , gms.points
  , gms.kill_points
  , gms.damage_dealt
  , gms.damage_taken
  , row_number() over (partition by gms.tournament_week_key, gms.game_title_key, gms.team_number order by (gms.points + gms.kill_points) desc) as game_rank

from game_match_scores as gms
)

, total_games as (
select
  *
  , case
      when game_rank <= 5 then 'top_five'
      else 'not_top_five'
    end as game_qualifier

from row_number_games
)

select
  tg.tournament_week_key
  , tg.game_title_key
  , tg.team_number
  , tt.captain_athlete_user_key
  , tg.athlete_user_key
  , tg.game_qualifier
  , sum(tg.damage_dealt) as damage_dealt
  , sum(tg.damage_taken) as damage_taken
  , sum(tg.points) as placement_score
  , sum(tg.kill_points) as kill_points
  , sum(tg.points) + sum(tg.kill_points)  as total_score

from total_games as tg
left join {{ ref('tournament_teams') }}  as tt
  on tg.tournament_week_key = tt.tournament_week_key
  and tg.game_title_key = tt.game_title_key
  and tg.team_number = tt.team_number

group by 1,2,3,4,5,6
