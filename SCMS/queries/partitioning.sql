set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.enforce.bucketing=true;
SET hive.exec.max.dynamic.partitions=2048; //default=1000
SET hive.exec.max.dynamic.partitions.pernode=1024; //default=100


CREATE TABLE delivery_parti(
id string, project_code string, pq string, po_or_so string, asnordn string,
--country string,
managed_by string, fulfill_via string, vendor_inco_term string, shipment_mode string,
pq_first_sent_to_client_date string, po_sent_to_vendor_date string, scheduled_delivery_date string,
delivered_to_client_date string, delivery_recorded_date string, product_group string,
sub_classification string, vendor string, item_description string, moleculetest_type string,
brand string, dosage string, dosage_form string, unit_of_measure_per_pack string,
line_item_quantity string, line_item_value string, pack_price string,
unit_price string, manufacturing_site string, first_line_designation string,
weight_kilograms string, freight_cost_usd string, line_item_insurance_usd string
)
COMMENT 'This is the page view table'
PARTITIONED BY(country STRING)
CLUSTERED BY(project_code) SORTED BY(id) INTO 8 BUCKETS
ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    	LINES TERMINATED BY '\n'
STORED AS SEQUENCEFILE;


From delivery_history de
INSERT OVERWRITE TABLE delivery_parti PARTITION(country)
select
de.id, de.project_code, de.pq, de.po_or_so,
de.asnordn, de.country, de.managed_by,
de.fulfill_via, de.vendor_inco_term, de.shipment_mode,
de.pq_first_sent_to_client_date, de.po_sent_to_vendor_date,
de.scheduled_delivery_date, de.delivered_to_client_date,
de.delivery_recorded_date, de.product_group,
de.sub_classification, de.vendor,
de.item_description, de.moleculetest_type,
de.brand, de.dosage, de.dosage_form, de.unit_of_measure_per_pack,
de.line_item_quantity, de.line_item_value, de.pack_price,
de.unit_price, de.manufacturing_site, de.first_line_designation,
de.weight_kilograms, de.freight_cost_usd, de.line_item_insurance_usd
DISTRIBUTE BY country;

