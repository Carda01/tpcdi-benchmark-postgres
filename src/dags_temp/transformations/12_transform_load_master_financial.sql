-------------------------------------------------------------------
-- Truncate before loading
-------------------------------------------------------------------
TRUNCATE TABLE master.financial;

-------------------------------------------------------------------
-- Optimized Load into master.financial using UNION ALL
-- (splits the OR condition into two separate joins)
-------------------------------------------------------------------
INSERT INTO master.financial (
    sk_companyid,
    fi_year,
    fi_qtr,
    fi_qtr_start_date,
    fi_revenue,
    fi_net_earn,
    fi_basic_eps,
    fi_dilut_eps,
    fi_margin,
    fi_inventory,
    fi_assets,
    fi_liability,
    fi_out_basic,
    fi_out_dilut
)
-------------------------------------------------------------------
-- Part 1: Match on conameorcik = companyid
-------------------------------------------------------------------
SELECT
    c.sk_companyid AS sk_companyid,
    f.year::numeric      AS fi_year,
    f.quarter::numeric   AS fi_qtr,
    f.qtrstartdate::date    AS fi_qtr_start_date,
    f.revenue::numeric    AS fi_revenue,
    f.earnings::numeric   AS fi_net_earn,
    f.eps::numeric       AS fi_basic_eps,
    f.dilutedeps::numeric AS fi_dilut_eps,
    f.margin::numeric     AS fi_margin,
    f.inventory::numeric  AS fi_inventory,
    f.assets::numeric     AS fi_assets,
    f.liability::numeric  AS fi_liability,
    f.shout::numeric      AS fi_out_basic,
    f.dilutedshout::numeric AS fi_out_dilut
FROM processing.finwire_fin f
JOIN master.dimcompany c
   ON f.conameorcik = c.companyid::varchar
  AND left(pts, 8)::date >= c.effectivedate
  AND left(pts, 8)::date <  c.enddate

UNION ALL

-------------------------------------------------------------------
-- Part 2: Match on conameorcik = name
-------------------------------------------------------------------
SELECT
    c.sk_companyid AS sk_companyid,
    f.year::numeric      AS fi_year,
    f.quarter::numeric   AS fi_qtr,
    f.qtrstartdate::date    AS fi_qtr_start_date,
    f.revenue::numeric    AS fi_revenue,
    f.earnings::numeric   AS fi_net_earn,
    f.eps::numeric        AS fi_basic_eps,
    f.dilutedeps::numeric AS fi_dilut_eps,
    f.margin::numeric     AS fi_margin,
    f.inventory::numeric  AS fi_inventory,
    f.assets::numeric     AS fi_assets,
    f.liability::numeric  AS fi_liability,
    f.shout::numeric      AS fi_out_basic,
    f.dilutedshout::numeric AS fi_out_dilut
FROM processing.finwire_fin f
JOIN master.dimcompany c
   ON f.conameorcik = c.name
  AND left(f.pts, 8)::date >= c.effectivedate
  AND left(f.pts, 8)::date <  c.enddate
;
