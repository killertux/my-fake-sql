SHOW COLUMNS FROM `entity_metadata`
;|
SHOW COLUMNS FROM `entity_metadata`|
SHOW COLUMNS FROM `payment`
;|
SHOW COLUMNS FROM `payment`|
SHOW COLUMNS FROM `refund`
;|
SHOW COLUMNS FROM `refund`|
SHOW COLUMNS FROM `merchnat`
;|
SHOW COLUMNS FROM `merchant`|
SHOW COLUMNS FROM `merchnat_cfg`
;|
SHOW COLUMNS FROM `merchant_cfg`|
SHOW COLUMNS FROM `country`
;|
SHOW COLUMNS FROM `country`|
SHOW COLUMNS FROM `settlement`
;|
SHOW COLUMNS FROM `settlement`|
SHOW COLUMNS FROM `currency_rate`
;|
SHOW COLUMNS FROM `currency_rate`|
SHOW COLUMNS FROM `fee_spec`
;|
SHOW COLUMNS FROM `fee_spec`|
-- Other queryes|
SELECT
	payment_percentage_spread,
	settlement_percentage_spread,
	remessa_percentage_spread,
	payment_type_group_id
FROM
	merchant_custom_spread
WHERE
	merchant_id = 5268
	AND country_id = 29
ORDER BY id
;|
SELECT
	payment_percentage_spread,
	settlement_percentage_spread,
	remessa_percentage_spread,
	payment_type_group_id
FROM
	merchant_custom_spread
WHERE
	merchant_id = 5268
	AND country_id = 33
ORDER BY id
;|
SELECT
	payment_percentage_spread,
	settlement_percentage_spread,
	remessa_percentage_spread,
	payment_type_group_id
FROM
	merchant_custom_spread
WHERE
	merchant_id = 5268
	AND country_id = 48
ORDER BY id
;|
SELECT
	payment_percentage_spread,
	settlement_percentage_spread,
	remessa_percentage_spread,
	payment_type_group_id
FROM
	merchant_custom_spread
WHERE
	merchant_id = 5268
	AND country_id = 53
ORDER BY id
;|
SELECT
	payment_percentage_spread,
	settlement_percentage_spread,
	remessa_percentage_spread,
	payment_type_group_id
FROM
	merchant_custom_spread
WHERE
	merchant_id = 5268
	AND country_id = 154
ORDER BY id
;|
SELECT
	payment_percentage_spread,
	settlement_percentage_spread,
	remessa_percentage_spread,
	payment_type_group_id
FROM
	merchant_custom_spread
WHERE
	merchant_id = 5268
	AND country_id = 183
ORDER BY id
;|
SELECT
	payment_percentage_spread,
	settlement_percentage_spread,
	remessa_percentage_spread,
	payment_type_group_id
FROM
	merchant_custom_spread
WHERE
	merchant_id = 5268
	AND country_id = 184
ORDER BY id
;|
SELECT
	payment_percentage_spread,
	settlement_percentage_spread,
	remessa_percentage_spread,
	payment_type_group_id
FROM
	merchant_custom_spread
WHERE
	merchant_id = 5268
	AND country_id = 245
ORDER BY id
;|
