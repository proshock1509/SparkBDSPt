from pyspark.sql import SparkSession
from ast import literal_eval
import pyspark.sql.functions as fn
spark = SparkSession.builder.master("local").getOrCreate()
sc  = spark.sparkContext

uri = "mongodb://localhost/BDS.flatten_data"
#BẮT LỖI NẾU URI Không tồn tại


def transform_data(__uri):
    '''
    ĐỌC DỮ LIỆU TỪ BẢNG flatten_data
    Dữ liệu được đọc thông qua Sparksession read "mongo", option với uri của MongoDB
    Dataframe : df_orgdata chứa Data đã được làm phẳng từ JSON bị lồng
    '''
    df_orgdata = spark.read.format("mongo")\
        .option("spark.mongodb.input.uri", __uri).load()

    df_orgdata.createOrReplaceTempView("orgdata") # Dataframe -> SQL Frame




    '''
    LỖI MỘT PHẦN NHỎ DATA KHI ĐỌC VÀO DATAFRAME
    Lúc này sẽ có khoảng 2000 rows bị lỗi do ký tự đặc biệt nên sẽ được cho vào cột _corrupt_record, dữ liệu nào đọc 
    thành công thì _corrupt_record = null
    Thực hiện lấy _corrupt_record, và xóa ký tự lạ trong 3 cột : content, title, và contact_address
    Convert lại thành RDDS String và đọc vào DataFrame df_err
    '''

    if "_corrupt_record" in df_orgdata.columns :
        rdds_err  = spark.sql("SELECT _corrupt_record from orgdata WHERE _corrupt_record is not null").rdd

        def null_content(r): #Clear special character
            r["content"] = ""
            r["title"] = ""
            r['contact_address'] = ""
            return r
            #Convert rdd -> cleaned 

        rdds_err_dict = rdds_err\
            .map(lambda r: (r["_corrupt_record"]))\
            .map(lambda r: literal_eval(r))\
            .map(null_content)\
            .map(lambda r : str(r))
        df_err = sqlContext.read.json(rdds_err_dict)
        '''
        Hợp Data bị lỗi và không bị lỗi dưới dạng RDDS sau đó convert lại thành DataFrame
        '''
        rdd_full = sc.union([rdds,rdds_err_dict])
        df_full =spark.read.json(rdd_full)
        df_full.createOrReplaceTempView("df_full")
        data = spark.sql('''SELECT * FROM df_full WHERE _corrupt_record is null 
                        AND city_id = 1 AND contact_mobile_phone is not null''')




    '''
    Lọc Data ở Hà Nội và có dữ liệu về môi giới
    '''
    data = spark.sql('''SELECT * FROM orgdata WHERE city_id = 1 AND contact_mobile_phone is not null''')

    data.createOrReplaceTempView("data")



    #CHUẨN HÓA SURFACE_SIZE
    '''
    Dữ liệu về diện tích trải dài từ 0 - hơn 1000m, chúng ta sẽ tiến hành phân thành 3 loại : 
    - Dưới 50m
    - Từ 50-200m
    - Trên 200m
    Thành column : surface_level
    '''
    surface_idx_col = fn.when(fn.col("surface_size") > 200,"Trên 200m")\
        .when(fn.col("surface_size") > 50,"Từ 50-200m")\
        .when(fn.col("surface_size") > 0, "Dưới 50m")\
        .otherwise("Khác")
    #Chuẩn hóa PRICE
    '''
    Dữ liệu về giá cả sử dụng 2 đơn vị Tỷ và triệu/ tháng không đồng nhất, cũng như có một số đơn vị null :
    Chuẩn hóa triệu/ tháng giữ nguyên là triệu, còn đơn vị tỷ cũng sẽ đổi thành đơn vị triệu
    - Giá với đơn vị là tỷ : nhân 1000
    - SALE null => nhân 1000
    - Còn lại giữ giá 
    Thành price_trieu
    '''
    price_trieu_col = fn.when(fn.col("unit") == "tỷ", fn.col("price")* 1000)\
        .when((fn.col("unit").isNull()) & (fn.col("use_for") == "SALE"), fn.col("price")* 1000)\
        .otherwise(fn.col("price"))

    data1 = data.withColumn("surface_level", surface_idx_col).withColumn("price_trieu", price_trieu_col)



    '''
    CHUẨN HÓA PHÂN KHÚC GIÁ
    Dữ liệu về giá sau khi đưa về cùng đơn vị triệu, sẽ tiến hành xếp nhóm vào phân khúc :
    SALE :
    Từ Dưới 1 tỷ : Giá rẻ
    Từ 1-10 tỷ : Tầm trung
    Trên 10 tỷ : Cao cấp
    RENT :
    Từ Dưới 3 triệu : Giá rẻ
    Từ 3-7 triệu : Tầm trung
    Trên 7 triệu : Cao cấp
    '''
    price_idx_col = fn.when(fn.col("price_trieu") == 0, "Khác")\
    .when(((fn.col("use_for") == "RENT")&(fn.col("price_trieu") <= 3))|((fn.col("use_for") == "SALE")&(fn.col("price_trieu") <= 1000)), "Giá rẻ" )\
    .when(((fn.col("use_for") == "RENT")&(fn.col("price_trieu") <= 7))|((fn.col("use_for") == "SALE")&(fn.col("price_trieu") <= 10000)), "Tầm trung")\
    .otherwise("Cao cấp")

    nor_data = data1.withColumn("price_level",price_idx_col)


    '''
    nor_data : Dữ liệu đủ cột, thêm 2 cột được chuẩn hóa

    f_data : Chỉ có dữ liệu các cột : 
    contact_mobile_phone ,district_ten, use_for,
    type_re_name, price_trieu, price_level, surface_size,
    surface_level, gps_lat, gps_lon, timestamp
    '''
    nor_data.createOrReplaceTempView("nor_data")
    f_data = spark.sql('''SELECT contact_mobile_phone ,district_ten, use_for,
    type_re_name, price_trieu, price_level, surface_size,
    surface_level, gps_lat, gps_lon, timestamp FROM nor_data ''')

    return (nor_data,f_data)




def write_data_to_mongo(tupple_data):
    nor_data = tupple_data[0]
    f_data = tupple_data[1]
    #Write to mongoDB dữ liệu đủ cột vào bảng: BDS.cleanedDataFull
    nor_data.write.format("mongo")\
        .option("spark.mongodb.output.uri","mongodb://127.0.0.1/BDS.cleanedDataFull").mode("overwrite").save()
    print("Sucessfull to cleanedFullData")
    #Write to mongoDB dữ liệu hữu hạn cột vào bảng : BDS.queryData
    f_data.write.format("mongo")\
        .option("spark.mongodb.output.uri","mongodb://127.0.0.1/BDS.queryData").mode("overwrite").save()
    print("Successful to queryData")

# res = clean_data(uri)
# write_data(res)