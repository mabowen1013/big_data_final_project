import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().appName("Remove Null Rows").getOrCreate()

val df = spark.read.option("header","true").option("inferSchema","true").csv("cleaned_game_data.csv")

val nonNullDF = df.na.drop()

nonNullDF.show()

nonNullDF.write.option("header","true").csv("cleaned_game_data_2.csv")

spark.stop()
