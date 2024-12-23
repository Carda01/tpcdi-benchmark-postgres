INSERT INTO master.factwatches
WITH 
-- Identify securities added to and removed from the watch list
watches AS (
	SELECT
		w1.w_c_id, 
		TRIM(w1.w_s_symb) AS w_s_symb, 
		w1.w_dts::DATE AS dateplaced, 
		w2.w_dts::DATE AS dateremoved
	FROM processing.watchhistory w1
	LEFT JOIN processing.watchhistory w2
		ON w1.w_c_id = w2.w_c_id
		AND w1.w_s_symb = w2.w_s_symb
		AND w1.w_action = 'ACTV'
		AND w2.w_action = 'CNCL'
		AND w2.w_dts >= w1.w_dts -- Ensure cancellation happens after activation
)
SELECT
	c.sk_customerid AS sk_customerid,
	s.sk_securityid AS sk_securityid,
	d1.sk_dateid AS sk_dateid_dateplaced,
	COALESCE(d2.sk_dateid, NULL) AS sk_dateid_dateremoved,
	3 AS batchid
FROM watches w
-- Join to find the surrogate key for customer
JOIN master.dimcustomer c
	ON w.w_c_id = c.customerid
	AND c.effectivedate <= w.dateplaced
	AND w.dateplaced <= c.enddate
-- Join to find the surrogate key for security
JOIN master.dimsecurity s
	ON w.w_s_symb = s.symbol
	AND s.effectivedate <= w.dateplaced
	AND w.dateplaced <= s.enddate
-- Join to find the surrogate key for the date placed
JOIN master.dimdate d1
	ON w.dateplaced = d1.datevalue
-- Left join to find the surrogate key for the date removed, allowing NULL if not canceled
LEFT JOIN master.dimdate d2
	ON w.dateremoved = d2.datevalue;