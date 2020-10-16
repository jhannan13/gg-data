with pivot_teams as (
select
  tournament_week_key
  , game_title_key
  , team_number
  , 'captain' as role
  , captain_athlete_user_key as athlete_user_key
  , team_start_time as start_time_epoch
  , team_end_time as end_time_epoch

from {{ ref('tournament_teams') }}

union all

select
  tournament_week_key
  , game_title_key
  , team_number
  , 'first_pick' as role
  , team_athlete_2_user_key as athlete_user_key
  , team_start_time as start_time_epoch
  , team_end_time as end_time_epoch

from {{ ref('tournament_teams') }}

union all

select
  tournament_week_key
  , game_title_key
  , team_number
  , 'second_pick' as role
  , team_athlete_3_user_key as athlete_user_key
  , team_start_time as start_time_epoch
  , team_end_time as end_time_epoch

from {{ ref('tournament_teams') }}

union all

select
  tournament_week_key
  , game_title_key
  , team_number
  , 'third_pick' as role
  , team_athlete_4_user_key as athlete_user_key
  , team_start_time as start_time_epoch
  , team_end_time as end_time_epoch

from {{ ref('tournament_teams') }}
)

select
  pt.*
  , ua.platform
  , ua.python_tag
  , concat('https://my.callofduty.com/api/papi-client/crm/cod/v2/title/mw/platform/'
    , ua.platform,'/gamer/', pt.athlete_user_key, '/matches/wz/start/'
    , pt.start_time_epoch,'/end/', pt.end_time_epoch,'/details') as api_url
  , concat(platform,'/gamer/',athlete_user_key) as python_tag_clean


from pivot_teams as pt
left join {{ ref('user_attributes') }} as ua
  on lower(pt.athlete_user_key) = lower(ua.user_key)

order by 1,3
