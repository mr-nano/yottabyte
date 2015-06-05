CREATE EXTERNAL TABLE REPAIR_IN_DOLLARS(
  vehicleType string,
  code string,
  description string,
  currency string,
  amount double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/vaibhavk/output';


CREATE EXTERNAL TABLE IF NOT EXISTS MAX_VALUE_REPAIR(
  vehicleType string,
   currency string,
  amount double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/vaibhavk/max_value_repair';

INSERT OVERWRITE TABLE MAX_VALUE_REPAIR
  SELECT vehicleType, currency, max(amount)
  FROM REPAIR_IN_DOLLARS
  GROUP BY vehicleType, currency;