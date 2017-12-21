#Sys.setenv("JAVA_HOME" = "/usr/java")
library(sparklyr)
library(dplyr)
#library(magrittr)

sparkInit <- function()
{
  Sys.setenv("SPARK_MEM" = "30G")
  config <- spark_config()
  config$spark.driver.maxResultSize <- "4G"
  config$spark.driver.memory <- "2G"
  config$spark.executor.memory <- "2G"
  sc <- spark_connect(master = "local", config = config)
  sc
}

sparkLocalRemoteInit <- function()
{
  Sys.setenv("SPARK_HOME" = "/Users/destiny/Documents/development/spark-2.1.1-bin-hadoop2.7") 
  Sys.setenv("SPARK_MEM" = "30G")
  config <- spark_config()
  config$spark.driver.maxResultSize <- "4G"
  config$spark.driver.memory <- "2G"
  config$spark.executor.memory <- "2G"
  sc <- spark_connect(master = "spark://Event-Horizon.local:7077", config = config)
  sc 
}

sparkShutDown <- function(sc)
{
  spark_disconnect_all()
}


spark_detect_date_range <- function(spark_df)
{
  min_date <- collect(spark_df %>% summarize(min(as.Date(signeddate))))
  max_date <- collect(spark_df %>% summarize(max(as.Date(signeddate))))
  date_range <- c(min_date, max_date)
  #date_range
  print(paste0("Detected date range is ", date_range[1], " to ", date_range[2]))
}

spark_detect_psc_range <- function(spark_df)
{
  pscs <- collect(sparkdf %>% select(prod_or_serv_code) %>% distinct)   
  pscs
}



load_spark_csv <- function(sc, filename)
{##future dev note: file loading phase can be speeded up by using spark csv reader
  
  print("Reading in export")
  raw_df <<- spark_read_csv(sc, name = "sprkdf", filename,delimiter = "\t", header = TRUE, overwrite = TRUE)
  toc( )
  
  print("Performing socio-economic factor clean-up")
  ###Re-code NAs first!!!!!!!
  raw_df <<- raw_df %>% mutate(sbg_flag = if_else(is.na(sbg_flag) == TRUE, "FALSE", sbg_flag))
  raw_df <<- raw_df %>% mutate(women_owned_flag = if_else(is.na(women_owned_flag) == TRUE, "FALSE", women_owned_flag))
  raw_df <<- raw_df %>% mutate(veteran_owned_flag = if_else(is.na(veteran_owned_flag) == TRUE, "FALSE", veteran_owned_flag))
  raw_df <<- raw_df %>% mutate(minority_owned_business_flag = if_else(is.na(minority_owned_business_flag) == TRUE, "FALSE", minority_owned_business_flag))
  raw_df <<- raw_df %>% mutate(foreign_government = if_else(is.na(foreign_government) == TRUE, "FALSE", foreign_government))
  
  raw_df <<- raw_df %>% mutate( co_bus_size_determination_code = if_else(is.na( co_bus_size_determination_code) == TRUE, "FALSE",  co_bus_size_determination_code))
  raw_df <<- raw_df %>% mutate( foreign_funding_desc = if_else(is.na( foreign_funding_desc) == TRUE, "FALSE",  foreign_funding_desc))
  #special case: not binary: account for "NOT APPLICABLE" 
  raw_df <<- raw_df %>% mutate( foreign_funding_desc = if_else(foreign_funding_desc == "NOT APPLICABLE", "FALSE",  foreign_funding_desc))
 
  
  
  raw_df <<- raw_df %>% mutate(firm8a_joint_venture = if_else(is.na(firm8a_joint_venture) == TRUE, "FALSE", firm8a_joint_venture))
  raw_df <<- raw_df %>% mutate(dot_certified_disadv_bus = if_else(is.na(dot_certified_disadv_bus) == TRUE, "FALSE", dot_certified_disadv_bus))
  raw_df <<- raw_df %>% mutate(sdb = if_else(is.na(sdb) == TRUE, "FALSE", sdb))
  raw_df <<- raw_df %>% mutate(sdb_flag = if_else(is.na(sdb_flag) == TRUE, "FALSE", sdb_flag))
  raw_df <<- raw_df %>% mutate(hubzone_flag = if_else(is.na(hubzone_flag) == TRUE, "FALSE", hubzone_flag))
  raw_df <<- raw_df %>% mutate(sheltered_workshop_flag = if_else(is.na(sheltered_workshop_flag) == TRUE, "FALSE", sheltered_workshop_flag))
  raw_df <<- raw_df %>% mutate(srdvob_flag = if_else(is.na(srdvob_flag) == TRUE, "FALSE", srdvob_flag))
  raw_df <<- raw_df %>% mutate(other_minority_owned = if_else(is.na(other_minority_owned) == TRUE, "FALSE", other_minority_owned))
  raw_df <<- raw_df %>% mutate(baob_flag = if_else(is.na(baob_flag) == TRUE, "FALSE", baob_flag))
  raw_df <<- raw_df %>% mutate(aiob_flag = if_else(is.na(aiob_flag) == TRUE, "FALSE", aiob_flag))
  raw_df <<- raw_df %>% mutate(naob_flag = if_else(is.na(naob_flag) == TRUE, "FALSE", naob_flag))
  raw_df <<- raw_df %>% mutate(haob_flag = if_else(is.na(haob_flag) == TRUE, "FALSE", haob_flag))
  raw_df <<- raw_df %>% mutate(saaob_flag = if_else(is.na(saaob_flag) == TRUE, "FALSE", saaob_flag))
  raw_df <<- raw_df %>% mutate(emerging_small_business_flag = if_else(is.na(emerging_small_business_flag) == TRUE, "FALSE", emerging_small_business_flag))
  raw_df <<- raw_df %>% mutate(wosb_flag = if_else(is.na(wosb_flag) == TRUE, "FALSE", wosb_flag))
  raw_df <<- raw_df %>% mutate( edwosb_flag = if_else(is.na(edwosb_flag) == TRUE, "FALSE",  edwosb_flag))
  raw_df <<- raw_df %>% mutate(jvwosb_flag = if_else(is.na(jvwosb_flag) == TRUE, "FALSE", jvwosb_flag))
  raw_df <<- raw_df %>% mutate(edjvwosb_flag = if_else(is.na(edjvwosb_flag) == TRUE, "FALSE", edjvwosb_flag))
  
  ##if not NA re-code flag to flag name or FALSE
  raw_df <<- raw_df %>% mutate(sbg_flag = if_else(sbg_flag=="Y", "SBG", "FALSE"))
  raw_df <<- raw_df %>% mutate(women_owned_flag = if_else(women_owned_flag == "YES", "WO", "FALSE"))
  raw_df <<- raw_df %>% mutate(veteran_owned_flag = if_else(veteran_owned_flag == "YES", "VO", "FALSE"))
  raw_df <<- raw_df %>% mutate(minority_owned_business_flag = if_else(minority_owned_business_flag == "YES", "MB", "FALSE"))
  raw_df <<- raw_df %>% mutate(foreign_government = if_else(foreign_government == "YES", "FG", "FALSE"))
  
  raw_df <<- raw_df %>% mutate( co_bus_size_determination_code = if_else(co_bus_size_determination_code == "S", "CO_SB",  "FALSE"))
  raw_df <<- raw_df %>% mutate( ff_fms = if_else(foreign_funding_desc == "FOREIGN FUNDS FMS", "FMS", "FALSE"))
  #raw_df <<- raw_df %>% mutate( foreign_funding_desc = ( foreign_funding_desc == "YES", "FOR_FND",  "FALSE"))
  #requires further eval based on how we will handle foreign funding
  raw_df <<- raw_df %>% mutate(firm8a_joint_venture = if_else(firm8a_joint_venture == "YES", "F8AJV", "FALSE"))
  raw_df <<- raw_df %>% mutate(dot_certified_disadv_bus = if_else(dot_certified_disadv_bus == "YES", "DOTCDB", "FALSE"))
  raw_df <<- raw_df %>% mutate(sdb = if_else(sdb == "YES", "SDB", "FALSE"))
  raw_df <<- raw_df %>% mutate(sdb_flag = if_else(sdb_flag == "YES", "SDB_FLAG", "FALSE"))
  raw_df <<- raw_df %>% mutate(hubzone_flag = if_else(hubzone_flag == "YES", "HUBZ", "FALSE"))
  raw_df <<- raw_df %>% mutate(sheltered_workshop_flag = if_else(sheltered_workshop_flag == "YES", "SWS", "FALSE"))
  raw_df <<- raw_df %>% mutate(srdvob_flag = if_else(srdvob_flag == "YES", "SRDVOB", "FALSE"))
  raw_df <<- raw_df %>% mutate(other_minority_owned = if_else(other_minority_owned == "YES", "OMO", "FALSE"))
  raw_df <<- raw_df %>% mutate(baob_flag = if_else(baob_flag == "YES", "BAOB", "FALSE"))
  raw_df <<- raw_df %>% mutate(aiob_flag = if_else(aiob_flag == "YES", "AIOB", "FALSE"))
  raw_df <<- raw_df %>% mutate(naob_flag = if_else(naob_flag == "YES", "NAOB", "FALSE"))
  raw_df <<- raw_df %>% mutate(haob_flag = if_else(haob_flag == "YES", "HAOB", "FALSE"))
  raw_df <<- raw_df %>% mutate(saaob_flag = if_else(saaob_flag == "YES", "SAAOB", "FALSE"))
  raw_df <<- raw_df %>% mutate(emerging_small_business_flag = if_else(emerging_small_business_flag == "YES", "ESB", "FALSE"))
  raw_df <<- raw_df %>% mutate(wosb_flag = if_else(wosb_flag == "YES", "WOSB", "FALSE"))
  raw_df <<- raw_df %>% mutate( edwosb_flag = if_else(edwosb_flag == "YES", "EDWOSB",  "FALSE"))
  raw_df <<- raw_df %>% mutate(jvwosb_flag = if_else(jvwosb_flag == "YES", "JVWOSB", "FALSE"))
  raw_df <<- raw_df %>% mutate(edjvwosb_flag = if_else(edjvwosb_flag == "YES", "EDJVWOSB", "FALSE"))
  
 
  
}


load_spark_parquet <- function(archive)
{
  
  print("Reading in export")
  ld_df <<- spark_read_parquet(sc, name="raw_df", path = archive)
  #filter by date range to only have FY16
  raw_df <<- ld_df %>% filter(level_1_category_group == "GWCM")
  print("subsetting training transactions")
  training_transactions <- raw_df %>% filter(as.Date(date_signed) >= as.Date("2012-10-01") & as.Date(date_signed) <= as.Date("2017-09-30")) 
  training_transactions <- training_transactions %>% mutate(addkey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government) )
  training_transactions <- training_transactions %>% mutate(psc_naics_key = paste0(product_or_service_code,"_",naics_code))
  training_transactions <<- training_transactions %>% mutate(case_multikey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government, "_", co_bus_size_determination_code, "_", foreign_funding_desc, "_", firm8a_joint_venture, "_", dot_certified_disadv_bus, "_", sdb, "_", sdb_flag, "_", hubzone_flag, "_", sheltered_workshop_flag,"_", srdvob_flag, "_", other_minority_owned, "_", baob_flag, "_", aiob_flag, "_", naob_flag, "_", haob_flag, "_", saaob_flag, "_", emerging_small_business_flag, "_", wosb_flag, "_", edwosb_flag, "_", jvwosb_flag, "_", edjvwosb_flag))

  testing_transactions <- raw_df %>% filter(as.Date(date_signed) >= as.Date("2016-10-01") & as.Date(date_signed) <= as.Date("2017-09-30")) 
  testing_transactions <- testing_transactions %>% mutate(addkey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government) )
  testing_transactions <- testing_transactions %>% mutate(psc_naics_key = paste0(product_or_service_code,"_",naics_code))
  testing_transactions <<- testing_transactions %>% mutate(case_multikey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government, "_", co_bus_size_determination_code, "_", foreign_funding_desc, "_", firm8a_joint_venture, "_", dot_certified_disadv_bus, "_", sdb, "_", sdb_flag, "_", hubzone_flag, "_", sheltered_workshop_flag,"_", srdvob_flag, "_", other_minority_owned, "_", baob_flag, "_", aiob_flag, "_", naob_flag, "_", haob_flag, "_", saaob_flag, "_", emerging_small_business_flag, "_", wosb_flag, "_", edwosb_flag, "_", jvwosb_flag, "_", edjvwosb_flag))

  
}


create_mini <- function(incoming_df)
{
  mini_df <- raw_df %>% select(award_type_description, co_bus_size_determination_code, date_signed, eight_a_flag, funding_agency_name, funding_department_name,
                               minority_owned_business_flag, naics_code, naics_description, piid, idv_ref_idv_piid, product_or_service_code, 
                               reference_piid, small_business_flag, level_1_category, veteran_owned_flag, women_owned_flag,
                               dollars_obligated, sbg_flag, sheltered_workshop_flag, foreign_government, foreign_funding_desc, sdb_flag, srdvob_flag,hubzone_flag,
                               firm8a_joint_venture, dot_certified_disadv_bus, sdb, other_minority_owned, baob_flag, aiob_flag, naob_flag, haob_flag, saaob_flag,
                               emerging_small_business_flag, wosb_flag, edwosb_flag, jvwosb_flag, contract_name, managing_agency, business_rule_tier, edjvwosb_flag, level_1_category_group, funding_cfo_act_agency)

                                 
}


sparkRInit <- function()
{
  
  Sys.setenv(SPARK_HOME = "/Users/cliftonbest/spark/spark-2.1.0-bin-hadoop2.7")
  
  library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
  sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "2g"))
}

sparkRLoadParquet <- function()
{
  ld_df <- read.df("../CombinedAddressability/04DEC17FY13_17.prq/")
  raw_df <<- ld_df %>% SparkR::filter(ld_df$level_1_category_group == "GWCM")
  print("subsetting training transactions")
  training_transactions <- raw_df %>% 
    SparkR::filter(date_format(raw_df$date_signed, 'yyyy-MM-dd') >= as.Date("2012-10-01") & date_format(raw_df$date_signed, 'yyyy-MM-dd') <= as.Date("2017-09-30"))  
  training_transactions <- training_transactions %>% 
    SparkR::mutate( addkey = concat_ws(sep = "_", training_transactions$product_or_service_code,training_transactions$naics_code, training_transactions$sbg_flag, training_transactions$women_owned_flag, training_transactions$veteran_owned_flag, training_transactions$minority_owned_business_flag, training_transactions$foreign_government) )
  training_transactions <- training_transactions %>% SparkR::mutate( psc_naics_key = concat_ws(sep = "_", training_transactions$product_or_service_code,training_transactions$naics_code))
  training_transactions <<- training_transactions %>% 
    SparkR::mutate( case_multikey = concat_ws(sep = "_",training_transactions$product_or_service_code,training_transactions$naics_code, training_transactions$sbg_flag, training_transactions$women_owned_flag, training_transactions$veteran_owned_flag, training_transactions$minority_owned_business_flag, training_transactions$foreign_government,  training_transactions$co_bus_size_determination_code,  training_transactions$foreign_funding_desc,  training_transactions$firm8a_joint_venture,  training_transactions$dot_certified_disadv_bus,  training_transactions$sdb,  training_transactions$sdb_flag,  training_transactions$hubzone_flag,  training_transactions$sheltered_workshop_flag, training_transactions$srdvob_flag,  training_transactions$other_minority_owned,  training_transactions$baob_flag,  training_transactions$aiob_flag,  training_transactions$naob_flag,  training_transactions$haob_flag,  training_transactions$saaob_flag,  training_transactions$emerging_small_business_flag,  training_transactions$wosb_flag,  training_transactions$edwosb_flag,  training_transactions$jvwosb_flag,  training_transactions$edjvwosb_flag))
  
  testing_transactions <- raw_df %>% 
    SparkR::filter(date_format(raw_df$date_signed, 'yyyy-MM-dd') >= as.Date("2016-10-01") & date_format(raw_df$date_signed, 'yyyy-MM-dd') <= as.Date("2017-09-30")) 
  testing_transactions <- training_transactions %>% 
    SparkR::mutate( addkey = concat_ws(sep = "_", training_transactions$product_or_service_code,training_transactions$naics_code, training_transactions$sbg_flag, training_transactions$women_owned_flag, training_transactions$veteran_owned_flag, training_transactions$minority_owned_business_flag, training_transactions$foreign_government) )
  testing_transactions <- training_transactions %>% 
    mutate( psc_naics_key = concat_ws(sep = "_", training_transactions$product_or_service_code,training_transactions$naics_code))
  testing_transactions <<- training_transactions %>% 
    mutate( case_multikey = concat_ws(sep = "_",training_transactions$product_or_service_code,training_transactions$naics_code, training_transactions$sbg_flag, training_transactions$women_owned_flag, training_transactions$veteran_owned_flag, training_transactions$minority_owned_business_flag, training_transactions$foreign_government,  training_transactions$co_bus_size_determination_code,  training_transactions$foreign_funding_desc,  training_transactions$firm8a_joint_venture,  training_transactions$dot_certified_disadv_bus,  training_transactions$sdb,  training_transactions$sdb_flag,  training_transactions$hubzone_flag,  training_transactions$sheltered_workshop_flag, training_transactions$srdvob_flag,  training_transactions$other_minority_owned,  training_transactions$baob_flag,  training_transactions$aiob_flag,  training_transactions$naob_flag,  training_transactions$haob_flag,  training_transactions$saaob_flag,  training_transactions$emerging_small_business_flag,  training_transactions$wosb_flag,  training_transactions$edwosb_flag,  training_transactions$jvwosb_flag,  training_transactions$edjvwosb_flag))
  
}

