The extrapolation for roamers need an up-to-date mcc-mnc table. The mcc-mnc-table has to be updated based on a certain time interval. 
Two data sources are used: 

1) https://github.com/musalbas/mcc-mnc-table  is a maintained repository that provides MCC/MNC csv.
Updated monthly from http://mcc-mnc.com/.

2) https://github.com/pbakondy/mcc-mnc-list extracts data from https://en.wikipedia.org/wiki/Mobile_country_code 

Data from http://mcc-mnc.com/ is used as the main data source. If there is any gaps or missing information, we use wikipedia data to add on. Manual checks are necessary to make sure data consistency between two data sources. 
