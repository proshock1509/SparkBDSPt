import pandas as pd 
from data_call import a
df = pd.read_csv("mgtheoquan.csv", sep = ",")

df["district_ten"]
data = {
    "district_ten" : df["district_ten"].tolist(),
    "sl" : df["sl"].tolist()
}

print(type(data))

print(a.cong(1,3))

from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .master("local") \
    .appName("Show data") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
rdd = spark.sparkContext.textFile('mgtheoquan.csv')
df = spark.read.csv(rdd)
print(df.show())

"{'city_id': 1, 'city_id_day_du': '1', 'city_tien_to': 'Thành phố', 'city_ten': 'Hà Nội',
 'city_ten_day_du': 'Thành phố Hà Nội', 'city_id_cha': 0, 'city_loai_dia_chi': 'tinh_tp', 
 'city_trang_thai': 'DA_PHE_DUYET', 'district_id': 20684, 'district_id_day_du': '1_20684', 
 'district_tien_to': 'Huyện', 'district_ten': 'Thanh Trì', 'district_ten_day_du': 
 'Huyện Thanh Trì, Thành phố Hà Nội', 'district_id_cha': 1, 'district_loai_dia_chi': 'quan_huyen',
  'district_trang_thai': 'GOP_TU_DONG', 'project_id': 534534, 'project_id_day_du': '835_20120_534534',
   'project_tien_to': 'Dự án', 'project_ten': 'Eco Green City', 
   'project_ten_day_du': 'Dự án Eco Green City, Huyện Thanh Trì, Tỉnh Hà Nội', 'project_id_cha': 39,
    'project_loai_dia_chi': 'du_an', 'project_trang_thai': 'GOP_TU_DONG', 'use_for': 'SALE',
     'type_re_id': 8, 'type_re_code': 'can_ho_chung_cu', 'type_re_name': 'Căn hộ, chung cư',
      'price': 2.7, 'unit': 'tỷ', 'price_per_meter': 29.347826, 'unit_price_per_meter': 'triệu/m2',
       'surface_size': 92.0, 'surface_width': 0.0, 'number_of_floors': 10.0, 'number_of_rooms': 3.0, 
       'number_of_toilets': 2.0, 'street_width': 0.0, 'submission_date': '2020-01-31', 
       'url': 'http://dothi.net/ban-can-ho-chung-cu-eco-green-city/chinh-chu-can-ban-can-ho-eco-green-city-pr13047562.htm',
        'orientation_id': 5, 'orientation_code': 'DN', 'orientation_ten': 'Đông Nam', 'id_mongo': '5e33d20a73f9f39cf72f2cfd',
        'title': 'Chính chủ cần bán căn hộ Eco Green City. DT 92m2, 3 PN, view đẹp & thoáng. LH 0963696996 a Trung',
         'domain': 'dothi.net', 'content': 'Chính chủ cần bán gấp căn 1009 CT2, 92m2 dự án Eco Green 2,7 tỷ.\\nDo không có nhu cầu sử dụng nên cần bán gấp căn 92m2 gồm 3PN, 2WC, cửa vào hướng Đông Nam, ban công hướng Tây Bắc. View đẹp và thoáng, tất cả các phòng đều lấy được ánh sáng tự nhiên.\\nGiá bán 2,7 tỷ.\\nLiên hệ chính chủ:\\xa00963696996. A TRUNG', 
         'contact': 'User_1580441337814', 'contact_mobile_phone': '0963696996', 'gps_lat': 20.98424, 'gps_lon': 105.8084,
          'timestamp': '2020-01-31 14:06:50', 'address': 'Dự án Eco Green City, Thanh Trì, Hà Nội ', 'zone': 'Bán căn hộ chung cư tại Eco Green City',
           'contact_address': 'Mỗ Lao, Hà Đông', 'day': 31}"


from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

working_directory = 'jars/*'

my_spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri=mongodb://127.0.0.1/test.myCollection") \
    .config("spark.mongodb.output.uri=mongodb://127.0.0.1/test.myCollection") \
    .getOrCreate()

people = my_spark.createDataFrame([("JULIA", 50), ("Gandalf", 1000), ("Thorin", 195), ("Balin", 178), ("Kili", 77),
                            ("Dwalin", 169), ("Oin", 167), ("Gloin", 158), ("Fili", 82), ("Bombur", 22)], ["name", "age"])

people.write.format("mongo").mode("append").save()

df = my_spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
df.select('*').where(col("name") == "JULIA").show()