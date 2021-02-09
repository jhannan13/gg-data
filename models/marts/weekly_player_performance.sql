with match_placements as (
select
  tournament_week_key
  , game_title_key
  , team_number
  , match_id
  , athlete_user_key
  , athlete
  , min(placement) as team_placement
  , sum(kills) as kills
  , sum(deaths) as deaths
  , sum(damage_done) as damage_dealt
  , sum(damage_taken) as damage_taken
  , sum(crates_opened) as crates_opened
  , sum(scavs_completed) as scavs_completed
  , sum(bounties_completed) as bounties_completed
  , sum(flags_completed) as flags_completed
  , sum(supply_runs_completed) as supply_runs_completed
  , sum(headshots) as headshots
  , sum(wall_bangs) as wall_bangs
  , sum(gulag_kills) as gulag_kills
  , sum(gulag_deaths) as gulag_deaths
  , sum(distance_traveled) as distance_traveled
  , sum(most_wanted) as most_wanteds

from {{ ref('tournament_gameplay') }}

group by 1,2,3,4,5,6
)

, game_match_scores as (
select
  mp.tournament_week_key
  , mp.game_title_key
  , mp.team_placement
  , mp.team_number
  , mp.athlete_user_key
  , mp.athlete
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
   , mp.kills
   , mp.deaths
   , mp.damage_dealt
   , mp.damage_taken
   , mp.crates_opened
   , mp.scavs_completed
   , mp.bounties_completed
   , mp.flags_completed
   , mp.supply_runs_completed
   , mp.headshots
   , mp.wall_bangs
   , mp.gulag_kills
   , mp.gulag_deaths
   , mp.distance_traveled
   , mp.most_wanteds

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
  , gms.athlete
  , gms.points
  , gms.kill_points
  , gms.kills
  , gms.deaths
  , gms.damage_dealt
  , gms.damage_taken
  , gms.crates_opened
  , gms.scavs_completed
  , gms.bounties_completed
  , gms.flags_completed
  , gms.supply_runs_completed
  , gms.headshots
  , gms.wall_bangs
  , gms.gulag_kills
  , gms.gulag_deaths
  , gms.distance_traveled
  , gms.most_wanteds
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
  , tg.athlete
  , dd.quartile
  , dd.kd_rank
  , tg.game_qualifier
  , sum(tg.damage_dealt) as damage_dealt
  , sum(tg.damage_taken) as damage_taken
  , sum(tg.points) as placement_score
  , sum(tg.kill_points) as kill_points
  , sum(tg.kills) as kills
  , sum(tg.deaths) as deaths
  , sum(tg.points) + sum(tg.kill_points)  as total_score
  , sum(tg.crates_opened) as crates_opened
  , sum(tg.scavs_completed) as scavs_completed
  , sum(tg.bounties_completed) as bounties_completed
  , sum(tg.flags_completed) as flags_completed
  , sum(tg.supply_runs_completed) as supply_runs_completed
  , sum(tg.headshots) as headshots
  , sum(tg.wall_bangs) as wall_bangs
  , sum(tg.gulag_kills) as gulag_kills
  , sum(tg.gulag_deaths) as gulag_deaths
  , sum(tg.distance_traveled) as distance_traveled
  , sum(tg.most_wanteds) as most_wanteds

from total_games as tg
left join {{ ref('tournament_teams') }}  as tt
  on tg.tournament_week_key = tt.tournament_week_key
  and tg.game_title_key = tt.game_title_key
  and tg.team_number = tt.team_number
left join {{ ref('draft_data') }} as dd
  on tg.tournament_week_key = dd.tournament_week_key
  and tg.athlete_user_key = dd.user_key

group by 1,2,3,4,5,6,7,8,9
