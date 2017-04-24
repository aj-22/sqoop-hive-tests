#!/bin/bash
# Shell script to execute hive queries for partitioning
# list.txt contains a list of countries
# Create table partitioned by country is a pre-requisite
# Values can be included via arguments 


partitionlist="./list.txt"
rawtable="scms.delivery_delim"
partable="scms.delivery_parti_static"
parcolumn="country"


while read -r line
do
        columnvalue="$line"
        my_query="insert into table $partable partition($parcolumn=\"$columnvalue\")"
        my_query="$my_query select id, project_code, pq, po_or_so,"
        my_query="$my_query asnordn, managed_by,"
        my_query="$my_query fulfill_via, vendor_inco_term,shipment_mode,"
        my_query="$my_query pq_first_sent_to_client_date, po_sent_to_vendor_date,"
        my_query="$my_query scheduled_delivery_date, delivered_to_client_date,"
        my_query="$my_query delivery_recorded_date, product_group,"
        my_query="$my_query sub_classification, vendor,"
        my_query="$my_query item_description, moleculetest_type,"
        my_query="$my_query brand, dosage, dosage_form, unit_of_measure_per_pack,"
        my_query="$my_query line_item_quantity, line_item_value, pack_price,"
        my_query="$my_query unit_price, manufacturing_site, first_line_designation,"
        my_query="$my_query weight_kilograms, freight_cost_usd, line_item_insurance_usd"
        my_query="$my_query from $rawtable og where og.$parcolumn=\"$columnvalue\";"
    my_value=$(hive -S -e "$my_query")
    echo "$my_value"
done < $partitionlist
