select
  athlete as athlete
  , discord as discord
  , platform as platform

from  {{ source('gg_raw', 'gg_raw_roster') }}
