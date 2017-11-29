#Sys.setenv("JAVA_HOME" = "/usr/java")
library(sparklyr)
library(dplyr)

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
  
  raw_df <<- raw_df %>% mutate(sbg_flag = if_else(sbg_flag=="Y", "SBG", "FALSE"))
  raw_df <<- raw_df %>% mutate(women_owned_flag = if_else(women_owned_flag == "YES", "WO", "FALSE"))
  raw_df <<- raw_df %>% mutate(veteran_owned_flag = if_else(veteran_owned_flag == "YES", "VO", "FALSE"))
  raw_df <<- raw_df %>% mutate(minority_owned_business_flag = if_else(minority_owned_business_flag == "YES", "MB", "FALSE"))
  raw_df <<- raw_df %>% mutate(foreign_government = if_else(foreign_government == "YES", "FG", "FALSE"))
  
  print("Creating add_key for all transactions")
  #filter by date range to only have FY16
  
  print("subsetting training transactions")
  training_transactions <<- raw_df %>% filter(as.Date(date_signed) >= as.Date("2013-10-01") & as.Date(date_signed) <= as.Date("2016-09-30"))
  
  
  print("subsetting testing transactions")
  testing_transactions <<- raw_df %>% filter(as.Date(date_signed) >= as.Date("2016-10-01") & as.Date(date_signed) <= as.Date("2017-09-30"))
  
}


load_spark_parquet <- function(archive)
{
  
  print("Reading in export")
  raw_df <<- spark_read_parquet(sc, name="raw_df", path = archive)
  #filter by date range to only have FY16
  
  print("subsetting training transactions")
  training_transactions <- raw_df %>% filter(as.Date(date_signed) >= as.Date("2012-10-01") & as.Date(date_signed) <= as.Date("2017-09-30")) 
  training_transactions <- training_transactions %>% mutate(addkey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government) )
  training_transactions <<- training_transactions %>% mutate(case_addkey = paste0(product_or_service_code,"_",naics_code))
  
  print("subsetting testing transactions")
  fy13testing_transactions <- raw_df %>% filter(as.Date(date_signed) >= as.Date("2012-10-01") & as.Date(date_signed) <= as.Date("2013-09-30")) 
  fy13testing_transactions <- fy13testing_transactions %>% mutate(addkey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government) )
  fy13testing_transactions <<- fy13testing_transactions %>% mutate(case_addkey = paste0(product_or_service_code,"_",naics_code))
  
  fy14testing_transactions <- raw_df %>% filter(as.Date(date_signed) >= as.Date("2013-10-01") & as.Date(date_signed) <= as.Date("2014-09-30")) 
  fy14testing_transactions <- fy14testing_transactions %>% mutate(addkey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government) )
  fy14testing_transactions <<- fy14testing_transactions %>% mutate(case_addkey = paste0(product_or_service_code,"_",naics_code))
  
  fy15testing_transactions <- raw_df %>% filter(as.Date(date_signed) >= as.Date("2014-10-01") & as.Date(date_signed) <= as.Date("2015-09-30")) 
  fy15testing_transactions <- fy15testing_transactions %>% mutate(addkey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government) )
  fy15testing_transactions <<- fy15testing_transactions %>% mutate(case_addkey = paste0(product_or_service_code,"_",naics_code))
  
  fy16testing_transactions <- raw_df %>% filter(as.Date(date_signed) >= as.Date("2015-10-01") & as.Date(date_signed) <= as.Date("2016-09-30")) 
  fy16testing_transactions <- fy16testing_transactions %>% mutate(addkey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government) )
  fy16testing_transactions <<- fy16testing_transactions %>% mutate(case_addkey = paste0(product_or_service_code,"_",naics_code))
  testing_transactions <- fy16testing_transactions
  testing_transactions <<- testing_transactions %>% mutate(case_addkey = paste0(product_or_service_code,"_",naics_code))
  
  fy17testing_transactions <- raw_df %>% filter(as.Date(date_signed) >= as.Date("2016-10-01") & as.Date(date_signed) <= as.Date("2017-09-30")) 
  fy17testing_transactions <- fy17testing_transactions %>% mutate(addkey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government) )
  fy17testing_transactions <<- fy17testing_transactions %>% mutate(case_addkey = paste0(product_or_service_code,"_",naics_code))
  
  
}
