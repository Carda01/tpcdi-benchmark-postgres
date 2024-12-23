BEGIN;

-- 2. Create temporary or helper indexes to speed up the big insert

-- Since we join dailymarket dm on (dm.dm_date = dd.datevalue)
-- and use dm_s_symb plus date in window functions, we index them:
CREATE INDEX IF NOT EXISTS idx_dailymarket_symb_date
    ON processing.dailymarket (dm_s_symb, dm_date);

-- Because we join dimdate dd on (dm.dm_date = dd.datevalue):
CREATE INDEX IF NOT EXISTS idx_dimdate_datevalue
    ON master.dimdate (datevalue);

-- Because we join dimsecurity s on (ld.dm_s_symb = s.symbol
--   AND ld.dm_date BETWEEN s.effectivedate AND s.enddate):
CREATE INDEX IF NOT EXISTS idx_dimsecurity_symbol_dates
    ON master.dimsecurity (symbol, effectivedate, enddate);

-- Because we join financial f on (s.sk_companyid = f.sk_companyid)
--   plus usage of f.fi_qtr_start_date in the "quarters" CTE:
CREATE INDEX IF NOT EXISTS idx_financial_qtrstart
    ON master.financial (sk_companyid, fi_qtr_start_date);

COMMIT;

BEGIN;

--factmarkethistory
TRUNCATE TABLE master.factmarkethistory;

INSERT INTO master.factmarkethistory
WITH market_dates_daily AS (
    SELECT 
        dm.dm_s_symb
      , dm.dm_date
      , dm.dm_close
      , dm.dm_high
      , dm.dm_low
      , dm.dm_vol
      , dd.sk_dateid
    FROM processing.dailymarket dm
    INNER JOIN master.dimdate dd 
        ON dm.dm_date = dd.datevalue
    ORDER BY dm.dm_s_symb, dm.dm_date DESC
),
high_low AS (
    SELECT
        dm_s_symb
      , dm_date
      , dm_close
      , dm_high
      , dm_low
      , dm_vol
      , MAX(dm_high) OVER (PARTITION BY dm_s_symb ORDER BY dm_date 
                           ROWS BETWEEN 363 PRECEDING AND CURRENT ROW) AS fiftytwoweekhigh
      , MIN(dm_low)  OVER (PARTITION BY dm_s_symb ORDER BY dm_date 
                           ROWS BETWEEN 363 PRECEDING AND CURRENT ROW) AS fiftytwoweeklow
    FROM market_dates_daily
),
high_date AS (
    SELECT
        hl.dm_s_symb
      , hl.dm_date
      , hl.dm_close
      , hl.dm_high
      , hl.dm_low
      , hl.dm_vol
      , hl.fiftytwoweekhigh
      , hl.fiftytwoweeklow
      , MAX(mdd.dm_date) AS sk_fiftytwoweekhighdate
    FROM high_low hl
    INNER JOIN market_dates_daily mdd
        ON hl.dm_s_symb = mdd.dm_s_symb
       AND hl.fiftytwoweekhigh = mdd.dm_high
       AND mdd.dm_date <= hl.dm_date
       AND mdd.dm_date >= hl.dm_date - INTERVAL '52 weeks'
    GROUP BY
        hl.dm_s_symb
      , hl.dm_date
      , hl.dm_close
      , hl.dm_high
      , hl.dm_low
      , hl.dm_vol
      , hl.fiftytwoweekhigh
      , hl.fiftytwoweeklow
),
low_date AS (
    SELECT
        hl.dm_s_symb
      , hl.dm_date
      , hl.dm_close
      , hl.dm_high
      , hl.dm_low
      , hl.dm_vol
      , hl.fiftytwoweekhigh
      , hl.fiftytwoweeklow
      , hl.sk_fiftytwoweekhighdate
      , MAX(mdd.dm_date) AS sk_fiftytwoweeklowdate
    FROM high_date hl
    INNER JOIN market_dates_daily mdd
        ON hl.dm_s_symb = mdd.dm_s_symb
       AND hl.fiftytwoweeklow = mdd.dm_low
       AND mdd.dm_date <= hl.dm_date
       AND mdd.dm_date >= hl.dm_date - INTERVAL '52 weeks'
    GROUP BY
        hl.dm_s_symb
      , hl.dm_date
      , hl.dm_close
      , hl.dm_high
      , hl.dm_low
      , hl.dm_vol
      , hl.fiftytwoweekhigh
      , hl.fiftytwoweeklow
      , hl.sk_fiftytwoweekhighdate
),
quarters AS (
    SELECT
        f.sk_companyid
      , f.fi_qtr_start_date
      , SUM(fi_basic_eps) OVER (
          PARTITION BY c.companyid
          ORDER BY f.fi_qtr_start_date
          ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS eps_qtr_sum
      , LEAD(fi_qtr_start_date, 1, '9999-12-31'::date) OVER (
          PARTITION BY c.companyid
          ORDER BY f.fi_qtr_start_date ASC
        ) AS next_qtr_start
    FROM master.financial f
    INNER JOIN master.dimcompany c
        ON f.sk_companyid = c.sk_companyid
),
final_output AS (
    SELECT
        s.sk_securityid
      , s.sk_companyid
      , TO_CHAR(ld.dm_date, 'yyyymmdd')::numeric AS sk_dateid
      , CASE
          WHEN q.eps_qtr_sum != 0 AND q.eps_qtr_sum IS NOT NULL
              THEN (ld.dm_close / q.eps_qtr_sum)::numeric(10, 2)
          ELSE NULL
        END AS peratio
      , CASE
          WHEN ld.dm_close != 0
              THEN ROUND((s.dividend / ld.dm_close) * 100, 2)
          ELSE NULL
        END AS yield
      , ld.fiftytwoweekhigh
      , TO_CHAR(ld.sk_fiftytwoweekhighdate, 'yyyymmdd')::numeric AS sk_fiftytwoweekhighdate
      , ld.fiftytwoweeklow
      , TO_CHAR(ld.sk_fiftytwoweeklowdate, 'yyyymmdd')::numeric AS sk_fiftytwoweeklowdate
      , ld.dm_close AS closeprice
      , ld.dm_high  AS dayhigh
      , ld.dm_low   AS daylow
      , ld.dm_vol   AS volume
      , 1 AS batchid
    FROM low_date ld
    INNER JOIN master.dimsecurity s
        ON ld.dm_s_symb = s.symbol
       AND ld.dm_date >= s.effectivedate
       AND ld.dm_date < s.enddate
    INNER JOIN quarters q
        ON s.sk_companyid = q.sk_companyid
       AND q.fi_qtr_start_date <= ld.dm_date
       AND q.next_qtr_start > ld.dm_date
)
SELECT *
FROM final_output;

COMMIT;

--------------------------------------------------
-- 5. Drop the indexes after the insert
--------------------------------------------------
BEGIN;

DROP INDEX IF EXISTS idx_dailymarket_symb_date;
DROP INDEX IF EXISTS idx_dimdate_datevalue;
DROP INDEX IF EXISTS idx_dimsecurity_symbol_dates;
DROP INDEX IF EXISTS idx_financial_qtrstart;

COMMIT;
