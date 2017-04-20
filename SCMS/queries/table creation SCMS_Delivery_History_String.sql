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
LOCATION '/user/cloudera/hive/scms/'

load data local inpath '/home/cloudera/Desktop/ajinkya/datasets/supply-chain-dummy/SCMS_Delivery_History_Dataset_20150929.csv' into table scms_delivery_history_string;