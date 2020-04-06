url = "cleaned_data.csv"
input_uri = "mongodb://127.0.0.1/BDS.queryData"
import pyspark.sql.functions as fn
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .master("local")\
    .appName("query")\
    .getOrCreate()
#rdd = sc.textFile(url)
schema = StructType([
    StructField("contact_mobile_phone", StringType()),
    StructField("district_ten", StringType()),
    StructField("use_for", StringType()),
    StructField("type_re_name", StringType()),
    StructField("price_trieu", StringType()),
    StructField("price_level", StringType()),
    StructField("surface_size", StringType()),
    StructField("surface_level", StringType()),
    StructField("gps_lat", StringType()),
    StructField("gps_lon", StringType()),
    StructField("timestamp", TimestampType())
])

df = spark.read.format("mongo")\
    .option("spark.mongodb.input.uri", input_uri)\
    .load()



#So luong moi gioi theo quan
def district_func():
    district_sl = df.groupby("district_ten")\
        .agg(fn.countDistinct("contact_mobile_phone").alias("sl"))\
        .sort("sl", ascending = False)
    district_sl.show(5)
    district_sl.write\
        .format("mongo")\
        .option("spark.mongodb.output.uri", "mongodb://localhost/BDS.district_sl_result")\
        .mode("overwrite")\
        .save()
    print("Successful!\n")
# district_func()

#So luong moi gioi theo muc dich trong cac quận
def district_usefor_func():
    district_usefor_sl_result = df.groupby("use_for", "district_ten")\
        .agg(fn.countDistinct("contact_mobile_phone").alias("sl"))\
        .sort(["use_for","sl"], ascending = False)
    district_usefor_sl_result.show()
    district_usefor_sl_result.write.format("mongo")\
        .option("spark.mongodb.output.uri","mongodb://localhost/BDS.district_usefor_sl_result")\
        .mode("overwrite")\
        .save()
    print("Successful!\n")
# district_usefor_func()


#So luong moi gioi theo loai trong cac quan
def district_type_func():
    district_type_sl_result = df.groupby("type_re_name", "district_ten")\
        .agg(fn.countDistinct("contact_mobile_phone").alias("sl"))\
        .sort(["type_re_name","sl"], ascending = False)
    district_type_sl_result.show()
    district_type_sl_result.write.format("mongo")\
        .option("spark.mongodb.output.uri", "mongodb://127.0.0.1/BDS.district_type_sl_result")\
        .mode("overwrite")\
        .save()
    print("Successful!\n")
# district_type_func()

#So luong moi gioi theo phan khuc từng quận
def district_price_func():
    district_price_sl_result = df.groupby( "price_level", "district_ten")\
        .agg(fn.countDistinct("contact_mobile_phone").alias("sl"))\
        .sort("price_level","sl", ascending = False)
    district_price_sl_result.show()
    district_price_sl_result.write\
        .format("mongo")\
        .option("spark.mongodb.output.uri", "mongodb://127.0.0.1/BDS.district_price_sl_result")\
        .mode("overwrite")\
        .save()
    print("Successful!\n")

# district_price_func()

#Số lượng môi giới diện tích từng quận
def district_surface_func():
    district_surface_sl_result = df.groupby("surface_level", "district_ten")\
        .agg(fn.countDistinct("contact_mobile_phone").alias("sl"))\
        .sort(["surface_level","sl"], ascending = False)
    district_surface_sl_result.show()
    district_surface_sl_result.write\
        .format("mongo")\
        .option("spark.mongodb.output.uri", "mongodb://localhost/BDS.district_surface_sl_result")\
        .mode("overwrite")\
        .save()
    print("Successful!\n")
# district_surface_func()


#So lượng môi giới : hoạt động trên các quận theo ngày 
def district_day_func():
    district_day_sl_result = df.groupby("district_ten", fn.date_format('timestamp', 'yyyy-MM-dd').alias('day'))\
        .agg(fn.countDistinct("contact_mobile_phone").alias("sl"))
    district_day_sl_result.show()
    district_day_sl_result.write.format("mongo")\
        .option("spark.mongodb.output.uri", "mongodb://localhost/BDS.district_day_sl_result")\
        .mode("overwrite")\
        .save()
    print("Successful!\n")
#district_day_func()







#So luong moi gioi theo muc dich thue/ban
def usefor_func():
    usefor_sl = df.groupby("use_for")\
        .agg(fn.countDistinct("contact_mobile_phone").alias("sl"))\
        .sort("sl", ascending = False)
    usefor_sl.write.format("mongo")\
        .option("spark.mongodb.output.uri", "mongodb://localhost/BDS.usefor_sl_result")\
        .mode("overwrite")\
        .save()
    usefor_sl.show()
    print("Successful!\n")
# usefor_func()


#So lượng môi giới : mục đích  + loại  
def usefor_type_func():
    usefor_type_sl_result = df.groupby("use_for", "type_re_name")\
        .agg(fn.countDistinct("contact_mobile_phone").alias("sl"))\
        .sort(["use_for", "sl"], ascending = False)
    usefor_type_sl_result.show()
    usefor_type_sl_result.write.format("mongo")\
        .option("spark.mongodb.output.uri","mongodb://localhost/BDS.usefor_type_sl_result")\
        .mode("overwrite")\
        .save()
    print("Successful!\n")
# usefor_type_func()


def usefor_price_func():
    usefor_price_sl_result = df.groupby("use_for", "price_level")\
        .agg(fn.countDistinct("contact_mobile_phone").alias("sl"))\
        .sort(["use_for", "sl"], ascending = False)
    usefor_price_sl_result.show()
    usefor_price_sl_result.write.format("mongo")\
        .option("spark.mongodb.output.uri", "mongodb://127.0.0.1/BDS.usefor_price_sl_result")\
        .mode("overwrite")\
        .save()
    print("Successful!\n")
# usefor_price_func()

#So lượng môi giới : mục đích  + diện tích 
def usefor_surface_func():
    usefor_surface_sl_result = df.groupby("use_for", "surface_level")\
        .agg(fn.countDistinct("contact_mobile_phone").alias("sl"))\
        .sort(["use_for", "sl"], ascending = False)

    usefor_surface_sl_result.show()
    usefor_surface_sl_result.write.format("mongo")\
        .option("spark.mongodb.output.uri", "mongodb://localhost/BDS.usefor_surface_sl_result")\
        .mode("overwrite")\
        .save()
    print("Successful!\n")
# usefor_surface_func()


#So lượng môi giới : mục đích  theo ngày trong tuần 
def usefor_day_func():
    usefor_day_sl_result = df.groupby("use_for",  fn.date_format('timestamp', 'yyyy-MM-dd').alias('day'))\
        .agg(fn.countDistinct("contact_mobile_phone").alias("sl"))\
        .sort("use_for")
    usefor_day_sl_result.show()
    usefor_day_sl_result.write.format("mongo")\
        .option("spark.mongodb.output.uri", "mongodb://localhost/BDS.usefor_day_sl_result")\
        .mode("overwrite")\
        .save()
    print("Successful!\n")
# usefor_day_func()













def type_func():
    type_sl_result = df.groupby("type_re_name")\
        .agg(fn.countDistinct("contact_mobile_phone").alias("sl"))\
        .sort("sl", ascending = False)
    type_sl_result.write.format("mongo")\
        .option("spark.mongodb.output.uri", "mongodb://127.0.0.1/BDS.type_sl_result")\
        .mode("overwrite")\
        .save()
# type_func()


def type_price_func():
    type_price_sl_result = df.groupby("type_re_name", "price_level")\
        .agg(fn.countDistinct("contact_mobile_phone").alias("sl"))\
        .sort(["type_re_name", "sl"], ascending = False)
    type_price_sl_result.show()
    type_price_sl_result.write.format("mongo")\
        .option("spark.mongodb.output.uri", "mongodb://127.0.0.1/BDS.type_price_sl_result")\
        .mode("overwrite")\
        .save()
# type_price_func()

#So lượng môi giới : mục đích  + diện tích 
def type_surface_func():
    type_surface_sl_result = df.groupby("type_re_name", "surface_level")\
        .agg(fn.countDistinct("contact_mobile_phone").alias("sl"))\
        .sort(["type_re_name", "sl"], ascending = False)

    type_surface_sl_result.show()
    type_surface_sl_result.write.format("mongo")\
        .option("spark.mongodb.output.uri", "mongodb://localhost/BDS.type_surface_sl_result")\
        .mode("overwrite")\
        .save()
# type_surface_func()

#So lượng môi giới : loại +  theo ngày 
def type_day_func():
    type_day_sl_result = df.groupby("type_re_name", fn.date_format('timestamp', 'yyyy-MM-dd').alias('day'))\
        .agg(fn.countDistinct("contact_mobile_phone").alias("sl"))\
        .sort("type_re_name")
    type_day_sl_result.show()
    type_day_sl_result.write.format("mongo")\
        .option("spark.mongodb.output.uri", "mongodb://127.0.0.1/BDS.type_day_sl_result")\
        .mode("overwrite")\
        .save()
# type_day_func()
















#So luong moi gioi theo phan khuc 
def price_func():
    price_sl_result = df.groupby("price_level")\
        .agg(fn.countDistinct("contact_mobile_phone").alias("sl"))\
        .sort("sl", ascending = False)
    price_sl_result.write.format("mongo")\
        .option("spark.mongodb.output.uri", "mongodb://127.0.0.1/BDS.price_sl_result")\
        .mode("overwrite")\
        .save()
# price_func()

#So lượng môi giới : mục đích  + diện tích 
def price_surface_func():
    price_surface_sl_result = df.groupby("price_level", "surface_level")\
        .agg(fn.countDistinct("contact_mobile_phone").alias("sl"))\
        .sort(["price_level", "sl"], ascending = False)

    price_surface_sl_result.show()
    price_surface_sl_result.write.format("mongo")\
        .option("spark.mongodb.output.uri", "mongodb://localhost/BDS.price_surface_sl_result")\
        .mode("overwrite")\
        .save()
# price_surface_func()

#So lượng môi giới : loại +  theo ngày 
def price_day_func():
    price_day_sl_result = df.groupby("price_level",  fn.date_format('timestamp', 'yyyy-MM-dd').alias('day'))\
        .agg(fn.countDistinct("contact_mobile_phone").alias("sl"))
    price_day_sl_result.show()
    price_day_sl_result.write.format("mongo")\
        .option("spark.mongodb.output.uri", "mongodb://127.0.0.1/BDS.price_day_sl_result")\
        .mode("overwrite")\
        .save()
# price_day_func()












#So luong moi gioi theo dien tich 
def surface_func():
    surface_sl_result = df.groupby("surface_level")\
        .agg(fn.countDistinct("contact_mobile_phone").alias("sl"))\
        .sort("sl", ascending = False)
    surface_sl_result.write.format("mongo")\
        .option("spark.mongodb.output.uri","mongodb://127.0.0.1/BDS.surface_sl_result")\
        .mode("overwrite")\
        .save()
    surface_sl_result.show()
# surface_func()


def surface_day_func():
    surface_day_sl_result = df.groupby("surface_level",fn.date_format('timestamp', 'yyyy-MM-dd').alias('day'))\
        .agg(fn.countDistinct("contact_mobile_phone").alias("sl"))
    surface_day_sl_result.show()
    surface_day_sl_result.write.format("mongo")\
        .option("spark.mongodb.output.uri", "mongodb://127.0.0.1/BDS.surface_day_sl_result")\
        .mode("overwrite")\
        .save()
# surface_day_func()

#So luong moi gioi theo ngay trong 1 tuan

def day_func():
    
    day_sl_result = df.groupby( fn.date_format('timestamp', 'yyyy-MM-dd').alias('day'))\
        .agg(fn.countDistinct("contact_mobile_phone").alias("sl"))
    day_sl_result.write.format("mongo")\
        .option("spark.mongodb.output.uri", "mongodb://127.0.0.1/BDS.day_sl_result")\
        .mode("overwrite")\
        .save()
# day_func()













#So lượng môi giới : mục đích  + phân khúc 




#So lượng môi giới : mục đích  + loại  + phân khúc
def usefor_type_price_func():
    usefor_type_price_sl_result = df.groupby('use_for',"type_re_name", "price_level")\
        .agg(fn.countDistinct("contact_mobile_phone").alias("sl"))\
        .sort("use_for","type_re_name", "price_level")

    usefor_type_price_sl_result.show()
    usefor_type_price_sl_result.write.format("mongo")\
        .option("spark.mongodb.output.uri", "mongodb://127.0.0.1/BDS.usefor_type_price_result")\
        .mode("overwrite")\
        .save()
# usefor_type_price_func()




