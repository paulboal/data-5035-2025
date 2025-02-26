/**
* This table summarizes the results of the data quality checks.
**/

SELECT * FROM {{ ref('dq_amount_overflow') }}
UNION ALL
SELECT * FROM {{ ref('dq_reversed_name') }}