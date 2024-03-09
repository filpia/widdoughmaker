CREATE OR REPLACE VIEW "bimonthly_snapshot" AS 
WITH
  bimonthly_records AS (
   SELECT
     *
   , date_parse(replace(split(happened_at, ' ')[1], '/', '-'), '%Y-%m-%d') happened_at_date
   , date_parse(concat(concat(concat(CAST(year AS varchar), '-'), (CASE WHEN (qtr = 'Q1') THEN '01' WHEN (qtr = 'Q2') THEN '04' WHEN (qtr = 'Q3') THEN '07' WHEN (qtr = 'Q4') THEN '10' END)), '-01'), '%Y-%m-%d') spirit_date
   FROM
     "AwsDataCatalog"."whiskey_invest_direct"."wid_prices_processed"
   WHERE (split(split(happened_at, ' ')[1], '/')[3] IN ('01', '15')) AND (currency = 'GBP')
) 
SELECT br.*
FROM
  (bimonthly_records br
INNER JOIN (
   SELECT
     happened_at_date
   , min(happened_at) happened_at
   FROM
     bimonthly_records
   GROUP BY happened_at_date
)  time_filt ON (br.happened_at = time_filt.happened_at))
;