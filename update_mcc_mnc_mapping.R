library(dplyr)
library(tidyr)
library(rjson)
library(jsonlite)

#########################Step 1: read in data sources ############################################

# read in the old table
oldtbl<-read.csv("mcc-mnc-table.csv")

# read in the new data downloaded from mcc-mnc.com (https://github.com/musalbas/mcc-mnc-table)
newtbl_web<-read.csv("mcc-mnc-table-master/mcc-mnc-table.csv")

# read in the new data downloaded from wikipedia (https://github.com/pbakondy/mcc-mnc-list)
newtbl_wikipedia<- fromJSON(paste(readLines("mcc-mnc-list-master/mcc-mnc-list.json"), collapse="")) %>% as.data.frame

#########################Step 2: check the discrepancy of country name in 2 data sources(mcc-mnc.com & wikipedia) #######
# check the discrepancy of country name in 2 tables
countryname_discrepancy<- newtbl_wikipedia %>%  
  group_by(countryName) %>% summarise() %>% ungroup() %>% 
  filter(!is.na(countryName)) %>% 
  mutate(source_wiki = 1) %>%
  full_join(newtbl_web %>% group_by(Country) %>% summarise() %>% ungroup() %>%
              mutate(source_web = 1), 
            by = c("countryName" = "Country")) %>%
  filter(is.na(source_wiki) | is.na(source_web)) 

'# Summary:
1) keep  French Antilles (France) in newtbl_wikipedia
2) change the following names in newtbl_wikipedia: 
   Hong Kong -->Hongkong
   Northern Mariana Islands (United States of America) -->United States of America
   Saint Helena, Ascension and Tristan da Cunha --> Saint Helena and Ascension and Tristan da Cunha
   United States Virgin Islands (United States of America) --> United States of America
   American Samoa (United States of America) --> United States of America
   Guam (United States of America) --> United States of America
   Guernsey (United Kingdom) --> United Kingdom
   Isle of Man (United Kingdom) --> United Kingdom
   Jersey (United Kingdom) --> United Kingdom
'

newnames_wikipedia<-countryname_discrepancy %>% 
  filter(source_wiki == 1 & countryName != "French Antilles (France)") %>% 
  mutate(countryName_new = case_when(countryName == "Hong Kong" ~ "Hongkong", 
                                     countryName == "Northern Mariana Islands (United States of America)" ~ "United States of America",
                                     countryName == "Saint Helena, Ascension and Tristan da Cunha" ~ "Saint Helena and Ascension and Tristan da Cunha",
                                     countryName == "United States Virgin Islands (United States of America)" ~ "United States of America",
                                     countryName == "American Samoa (United States of America)" ~ "United States of America",
                                     countryName == "Guam (United States of America)" ~ "United States of America",
                                     countryName == "Guernsey (United Kingdom)" ~ "United Kingdom",
                                     countryName == "Isle of Man (United Kingdom)" ~ "United Kingdom",
                                     countryName == "Jersey (United Kingdom)" ~ "United Kingdom",
                                     TRUE ~ countryName
                                     )) %>% 
  mutate(countryName_new = gsub("\\s*\\([^\\)]+\\)","",countryName_new)) 

# update the country name in wikipedia data source to be consistency with mcc-mnc.com 
newtbl_wikipedia<-newtbl_wikipedia %>% 
  left_join(newnames_wikipedia %>% select(countryName, countryName_new), by = "countryName") %>% 
  mutate(countryName = ifelse(!is.na(countryName_new), countryName_new, countryName)) %>% select(-countryName_new)

#########################Step 3: consolidate two data sources together and do cleaning job ##############

# combine 2 table together joining by MNC, MCC
combined_table_raw<-newtbl_web %>%  
  full_join(
    newtbl_wikipedia %>% 
      select(Country_wikipedia = countryName,
             MCC = mcc, 
             MNC = mnc,
             Network_wikipedia = brand) %>% 
      mutate(MNC = as.numeric(MNC),
             MCC = as.numeric(MCC)) , 
    by = c("MNC", "MCC")
  ) 

combined_table_processed<-combined_table_raw %>% 
  filter(!is.na(MNC) & !is.na(MCC)) %>% # only keep records where MCC and MNC is not null
  # if there is a Country name in mcc-mnc.com table, use it. Otherwise use the Country name in wikipedia (Same for Network)
  mutate(Country = ifelse(is.na(Country), Country_wikipedia, Country), 
         Network = ifelse(is.na(Network), Network_wikipedia, Network)) %>% 
  group_by(MCC, `MCC..int.`, MNC,`MNC..int.`, ISO, Country, `Country.Code`, Network) %>%
  summarise() %>% 
  ungroup() %>% 
  filter(!is.na(Country)) 

# manually check duplicated records (group by MCC, MNC, Network )
combined_table_processed %>% group_by(MCC, MNC, Network) %>% mutate(n = n()) %>% 
  ungroup() %>% filter(n > 1) %>% View

'# Summary: 
   1) Puerto Rico belongs to United States of America, so only keep United States of America, filter out Puerto Rico
   2) Filter out "Bonaire Sint Eustatius and Saba", only keep "Curacao" 
   3) Filter out Mayotte, only keep Reunion
'

combined_table_processed_new<-combined_table_processed %>% 
  filter(!(Country == "Puerto Rico" & Network == "ProxTel")) %>% 
  filter(!(Country == "Mayotte" & Network == "SFR")) %>% 
  filter(Country != "Bonaire Sint Eustatius and Saba")

# check duplicated records again for similar network name
combined_table_processed_new %>% group_by(MCC, MNC) %>% mutate(n = n()) %>% 
  ungroup() %>% filter(n > 1) %>% View

'# Summary: 
   1) Tele2 is now T-Mobile, so filter(!(Country == "Netherlands" & Network == "Tele2"))
   2) filter(Network != "KPN/Telfort")
   3) filter(!(MNC == 20 & Network == "T-Mobile" & Country == "Netherlands"))
   4) filter(!(MNC == 98 & Network == "T-Mobile" & Country == "Netherlands"))
'

#########################Step 4: Final cleaned data ##########################
'# Steps need to be done: 
   1) Check the country names in new combined table and oldtbl to make sure all the country names make sense. 
   2) Change Country "Guam" to "United States of America"
   3) Change "Macau" to "Macao"
   4) filter out Network name is "Failed Calls"/"Failed Call(s)"
   5) For records, where Network is null, put MCC-MNC
   6) For records where MCC and MNC are the same, but Network are different, put all Networks in an array with | as delimiter to keep only 1 record
   7) Change the Network name "Optus" to "Singtel Optus"
   8) make sure all mcc are unique to the country. If a mcc code has different country names, put all countries together with | as delimiter
'

combined_table_final<-combined_table_processed_new %>% 
  mutate(Country = case_when(Country == "Guam" ~ "United States of America", 
                             Country == "Macau" ~ "Macao", 
                             TRUE ~ Country)) %>% 
  filter(!Network %in% c("Failed Calls", "Failed Call(s)")) %>%
  filter(!(Country == "Netherlands" & Network == "Tele2")) %>% 
  filter(Network != "KPN/Telfort") %>% 
  filter(!(MNC == 20 & Network == "T-Mobile" & Country == "Netherlands")) %>%
  filter(!(MNC == 98 & Network == "T-Mobile" & Country == "Netherlands")) %>% 
  # change "Optus" to "Singtel Optus"
  mutate(Network = ifelse(Network == "Optus", "Singtel Optus", Network)) %>% 
  # put MCC-MNC where Network is Null
  mutate(Network = ifelse(is.na(Network)|Network == ""|Network == " ", paste(MCC, MNC, sep = "-"), Network)) %>% 
  # get Network array
  group_by(MCC, `MCC..int.`, MNC,`MNC..int.`, ISO, Country, `Country.Code`) %>%
  summarise(Network = paste(Network, collapse = "|")) %>% 
  ungroup()  %>% 
  group_by(MCC) %>% 
  mutate(Country = paste(unique(Country), collapse = "|")) %>% 
  ungroup() 
  
combined_table_final %>% write.csv("mcc-mnc-table-20220913.csv", row.names = FALSE)
