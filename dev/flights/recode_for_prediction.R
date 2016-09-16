#data_2008 %>% group_by(crsarrtime) %>% summarize(freq = n()) %>% arrange(desc(freq))
#mutate(uniquecarrier = ifelse(crsarrtime == 351, "DH", uniquecarrier)) %>%
#mutate(uniquecarrier = ifelse(crsarrtime == 120, "HP", uniquecarrier)) %>%
#mutate(uniquecarrier = ifelse(crsarrtime == 347, "TZ", uniquecarrier)) %>%
