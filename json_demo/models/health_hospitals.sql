SELECT
  seq,
  index,
  value:Hospital as hospital,
  value:Beds as beds,
  value:City as city
FROM
  {{ source('source', 'health') }},
  LATERAL FLATTEN(INPUT => hospitals:Hospitals)
