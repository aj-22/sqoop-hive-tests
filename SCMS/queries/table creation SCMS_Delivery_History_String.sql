--SCMS csv contains quoted "" data. We have to use SerDe to load quoted data.

CREATE TABLE `scms_delivery.delivery_history` (
  `ID` string ,`Project_Code` string ,`PQ` string ,`PO_or_SO` string ,
  `ASNorDN` string ,`Country` string ,`Managed_By` string ,`Fulfill_Via` string ,
  `Vendor_INCO_Term` string ,`Shipment_Mode` string ,`PQ_First_Sent_to_Client_Date` string ,
  `PO_Sent_to_Vendor_Date` string ,`Scheduled_Delivery_Date` string ,
  `Delivered_to_Client_Date` string ,`Delivery_Recorded_Date` string ,
  `Product_Group` string ,`Sub_Classification` string ,`Vendor` string ,
  `Item_Description` string ,`MoleculeTest_Type` string ,`Brand` string ,
  `Dosage` string ,`Dosage_Form` string ,`Unit_of_Measure_Per_Pack` string ,
  `Line_Item_Quantity` string ,`Line_Item_Value` string ,`Pack_Price` string ,
  `Unit_Price` string ,`Manufacturing_Site` string ,`First_Line_Designation` string ,
  `Weight_Kilograms` string ,`Freight_Cost_USD` string ,`Line_Item_Insurance_USD` string ) 
  COMMENT "SCMS Delivery HISTORY" ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
  WITH SERDEPROPERTIES ("separatorChar"=",","quoteChar"="\"") STORED AS TextFile

--The table below may not be loaded with consistent data

CREATE EXTERNAL TABLE SCMS_Delivery_History_String(
ID STRING,
Project_Code STRING,
PQ STRING,
PO_or_SO STRING,
ASNorDN STRING,
Country STRING,
Managed_By STRING,
Fulfill_Via STRING,
Vendor_INCO_Term STRING,
Shipment_Mode STRING,
PQ_First_Sent_to_Client_Date STRING,
PO_Sent_to_Vendor_Date STRING,
Scheduled_Delivery_Date STRING,
Delivered_to_Client_Date STRING,
Delivery_Recorded_Date STRING,
Product_Group STRING,
Sub_Classification STRING,
Vendor STRING,
Item_Description STRING,
MoleculeTest_Type STRING,
Brand STRING,
Dosage STRING,
Dosage_Form STRING,
Unit_of_Measure_Per_Pack STRING,
Line_Item_Quantity STRING,
Line_Item_Value STRING,
Pack_Price STRING,
Unit_Price STRING,
Manufacturing_Site STRING,
First_Line_Designation STRING,
Weight_Kilograms STRING,
Freight_Cost_USD STRING,
Line_Item_Insurance_USD STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS textfile
LOCATION '/user/cloudera/hive/scms/';

load data local inpath 
'/home/cloudera/Desktop/ajinkya/datasets/supply-chain-dummy/SCMS_Delivery_History_Dataset_20150929.csv' 
into table scms_delivery_history;
