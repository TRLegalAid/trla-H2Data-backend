CREATE MATERIALIZED VIEW previously_geocoded AS (
	SELECT DISTINCT ON (full_address)
        full_address, accuracy, accuracy_type, latitude, longitude
    	FROM (
    	SELECT
        LOWER(coalesce("HOUSING_ADDRESS_LOCATION", '') || COALESCE("HOUSING_CITY", '') || COALESCE("HOUSING_STATE", '') || COALESCE("HOUSING_POSTAL_CODE", '')) AS full_address,
    	"housing accuracy" AS accuracy, "housing accuracy type" AS accuracy_type, "housing_lat" AS latitude, "housing_long" AS longitude
    	FROM job_central
        UNION
        SELECT
    	LOWER(COALESCE("WORKSITE_ADDRESS", '') || COALESCE("WORKSITE_CITY", '') || COALESCE("WORKSITE_STATE", '') || COALESCE("WORKSITE_POSTAL_CODE", '')) AS full_address,
        "worksite accuracy" AS accuracy, "worksite accuracy type" AS accuracy_type, "worksite_lat" AS latitude, "worksite_long" AS longitude
    	FROM job_central
        UNION
        SELECT
        LOWER(COALESCE("HOUSING_ADDRESS_LOCATION", '') || COALESCE("HOUSING_CITY", '') || COALESCE("HOUSING_STATE", '') || COALESCE("HOUSING_POSTAL_CODE", '')) AS full_address,
    	"housing accuracy" AS accuracy, "housing accuracy type" AS accuracy_type, "housing_lat" AS latitude, "housing_long" AS longitude
    	FROM additional_housing
    	UNION
        SELECT
        LOWER(COALESCE("HOUSING_ADDRESS_LOCATION", '') || COALESCE("HOUSING_CITY", '') || COALESCE("HOUSING_STATE", '') || COALESCE("HOUSING_POSTAL_CODE", '')) AS full_address,
    	"housing accuracy" AS accuracy, "housing accuracy type" AS accuracy_type, "housing_lat" AS latitude, "housing_long" AS longitude
    	FROM low_accuracies
        UNION
        SELECT
    	LOWER(COALESCE("WORKSITE_ADDRESS", '') || COALESCE("WORKSITE_CITY", '') || COALESCE("WORKSITE_STATE", '') || COALESCE("WORKSITE_POSTAL_CODE", '')) AS full_address,
        "worksite accuracy" AS accuracy, "worksite accuracy type" AS accuracy_type, "worksite_lat" AS latitude, "worksite_long" AS longitude
        FROM low_accuracies
    	) AS addresses_view
		ORDER BY full_address
);

CREATE UNIQUE INDEX address_index ON previously_geocoded(full_address);

-- to refresh view
REFRESH MATERIALIZED VIEW previously_geocoded
