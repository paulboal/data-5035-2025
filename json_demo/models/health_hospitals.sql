SELECT
  seq,
  index,
  value:Hospital as hospital,
  value:Beds as beds,
  value:City as city
FROM
  {{ ref('health_raw') }},
  LATERAL FLATTEN(INPUT => system:Hospitals)
