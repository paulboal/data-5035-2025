/**
*  People can be generous, but we don't expect donations over six figures.
*  Flag any donations over 999,999.
**/

SELECT 
  UPPER('{{ model.name }}') AS DQ_CHECK,
  donation_id,
  CASE WHEN amount > 999999 THEN 1 ELSE 0 END as DQ_RESULT
FROM
  {{ ref('donations') }}
