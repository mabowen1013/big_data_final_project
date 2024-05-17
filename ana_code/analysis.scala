import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().appName("profiling data").master("local[*]").getOrCreate()

val data = spark.read.option("header","true").csv("cleaned_game_data_2.csv")

val processedData = data.withColumn("Peak CCU", col("Peak CCU").cast("int")).withColumn("Positive",col("Positive").cast("int")).withColumn("Negative", col("Negative").cast("int")).withColumn("Price",col("Price").cast("float")).withColumn("userScore",col("User score").cast("int"))


val correlationData = processedData.stat.corr("Positive", "Peak CCU")
val corrPriceUserScore = processedData.stat.corr("price", "userScore")
val corrPriceNegative = processedData.stat.corr("price", "negative")
val corrPositiveNegative = processedData.stat.corr("positive", "negative")

println(s"Correlation between positive reviews and peak concurrent users: $correlationData")
println(s"Correlation between price and user score: $corrPriceUserScore")
println(s"Correlation between price and negative reviews: $corrPriceNegative")
println(s"Correlation between positive and negative reviews: $corrPositiveNegative")

val processedData = data.withColumn("peak CCU", col("Peak CCU").cast("int")).withColumn("Average playtime forever", col("Median playtime forever").cast("int")).withColumn("Median playtime forever", col("Median playtime forever").cast("int")).withColumn("Positive", col("Positive").cast("int")).withColumn("Negative", col("Negative").cast("int")).withColumn("Price", col("Price").cast("float"))


val corrPlaytimePrice = processedData.stat.corr("Average playtime forever", "Price")
val corrPlaytimePositive = processedData.stat.corr("Average playtime forever", "Positive")

println(s"Correlation between average playtime and price: $corrPlaytimePrice")
println(s"Correlation between average playtime and positive reviews: $corrPlaytimePositive")


val processedData = data.withColumn("User score", col("User score").cast("int"))

val scoreFrequency = processedData.groupBy("User score").count().orderBy("User score")


scoreFrequency.show()


val processedData = data.withColumn("Positive",col("Positive").cast("int")).withColumn("Negative",col("Negative").cast("int"))

val withPositiveRatio = processedData.withColumn("positive_ratio",col("Positive")/(col("Positive")+col("Negative")+0.0001))

withPositiveRatio.select("positive_ratio").show(10)

























































