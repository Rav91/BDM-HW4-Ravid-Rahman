import csv
import pyspark
from pyspark.sql import SparkSession
from statistics import pstdev, median
from pyspark.sql.functions import year, to_date, monotonically_increasing_id, row_number
from pyspark.sql.window import Window

def main(sc):
    spark = SparkSession(sc)
    
    core_places_df = spark.read.csv('data/share/bdm/core-places-nyc.csv', header=True, escape='"')

    core_places_df = core_places_df.select('placekey', 'location_name', 'naics_code')

    big_box_df = core_places_df.filter('naics_code == 452210' or 'naics_code == 452311')
    conv_store_df = core_places_df.filter('naics_code == 445120')
    drinking_df = core_places_df.filter('naics_code == 722410')
    full_service_df = core_places_df.filter('naics_code == 722511')
    limited_service_df = core_places_df.filter('naics_code == 722513')
    pharma_and_drug_df = core_places_df.filter('naics_code == 446110' or 'naics_code == 446191')
    snack_and_baker_df = core_places_df.filter('naics_code == 311811' or 'naics_code == 722515')
    spec_food_df = core_places_df.filter('naics_code == 445210' or 'naics_code == 445220' or 'naics_code == 445230'
                                      or 'naics_code == 445291' or 'naics_code == 445292' or 'naics_code == 445299')
    super_sans_conv_df = core_places_df.filter('naics_code == 445110')
    
    weekly_pattern_df = spark.read.csv('data/share/bdm/weekly-patterns-nyc-2019-2020/*', header=True, escape='"')
    weekly_pattern_df = weekly_pattern_df.select('placekey', 'date_range_start', 'visits_by_day')
    
    visits_by_day = weekly_pattern_df.select('visits_by_day').rdd.map(lambda row : row[0]).collect()
    stdv_median = []
    for i in range((len(visits_by_day))):
      stdv_median.append((int(statistics.pstdev(list(map(int, visits_by_day[i].replace('[', '').replace(']', '').split(','))))),
                   int(statistics.median(list(map(int, visits_by_day[i].replace('[', '').replace(']', '').split(',')))))))

    stdv_median_df = spark.createDataFrame(stdv_median, schema=['stdv', 'median'])
    stdv_median_df =  stdv_median_df.withColumn("index", row_number().over(w))
    weekly_pattern_df =  weekly_pattern_df.withColumn("index", row_number().over(w))

    weekly_pattern_df = weekly_pattern_df.join(stdv_median_df, weekly_pattern_df.index == stdv_median_df.index,'inner')

    weekly_pattern_df = weekly_pattern_df.withColumn('year', year(weekly_pattern_df.date_range_start))
    weekly_pattern_df = weekly_pattern_df.withColumn('date', to_date(weekly_pattern_df.date_range_start))
    weekly_pattern_df = weekly_pattern_df.withColumn('high', (weekly_pattern_df.median + weekly_pattern_df.stdv))
    weekly_pattern_df = weekly_pattern_df.withColumn('low', (weekly_pattern_df.median - weekly_pattern_df.stdv))
    weekly_pattern_df = weekly_pattern_df.drop('visits_by_day', 'date_range_start', 'index', 'stdv')
    
    big_box_weekly_pattern_df = weekly_pattern_df.join(big_box_df, ['placekey'], 'leftsemi')
    conv_store_weekly_pattern_df = weekly_pattern_df.join(conv_store_df, ['placekey'], 'leftsemi')
    drinking_weekly_pattern_df = weekly_pattern_df.join(drinking_df, ['placekey'], 'leftsemi')
    full_service_weekly_pattern_df = weekly_pattern_df.join(full_service_df, ['placekey'], 'leftsemi')
    limited_service_weekly_pattern_df = weekly_pattern_df.join(limited_service_df, ['placekey'], 'leftsemi')
    pharma_and_drug_weekly_pattern_df = weekly_pattern_df.join(pharma_and_drug_df, ['placekey'], 'leftsemi')
    snack_and_baker_weekly_pattern_df = weekly_pattern_df.join(snack_and_baker_df, ['placekey'], 'leftsemi')
    spec_food_weekly_pattern_df = weekly_pattern_df.join(spec_food_df, ['placekey'], 'leftsemi')
    super_sans_conv_weekly_pattern_df = weekly_pattern_df.join(super_sans_conv_df, ['placekey'], 'leftsemi')
    
    big_box_weekly_pattern_df.write.csv('big_box_grocers.csv')
    conv_store_weekly_pattern_df.write.csv('convience_store.csv')
    drinking_weekly_pattern_df.write.csv('drinking_places.csv')
    full_service_weekly_pattern_df.write.csv('full_service_resturants.csv')
    limited_service_weekly_pattern_df.write.csv('limited_service_resturants.csv')
    pharma_and_drug_weekly_pattern_df.write.csv('pharmacies_and_drug_stores.csv')
    snack_and_baker_weekly_pattern_df.write.csv('snack_and_bakeries.csv')
    spec_food_weekly_pattern_df.write.csv('specialty_food_stores.csv')
    super_sans_conv_weekly_pattern_df.write.csv('supermakets_except_convenience_stores.csv')
    
if __name__ == "__main__":
    sc = pyspark.SparkContext()
    main(sc)
