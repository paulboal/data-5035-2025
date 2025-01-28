/**
*  We expect the NAME field to be in the format "First Last" or "First Middle Last".
*  Check to see if the name field has a reversed name.
*  We can identify reversed names by checking if a comma appears in the NAME field.
**/

SELECT 
  UPPER('{{ model.name }}') AS DQ_CHECK,
  donation_id,
  CASE WHEN CONTAINS(name, ',') THEN 1 ELSE 0 END AS DQ_RESULT
FROM
  {{ ref('donations') }}
