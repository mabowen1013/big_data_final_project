import org.apache.spark.sql.SparkSession


val spark = SparkSession.builder().appName("Remove Columns").getOrCreate()

val df = spark.read.option("header", "true").option("inferSchema", "true").csv("game_data.csv.csupload")



df.printSchema()

val columnsToKeep = df.columns.filterNot(column => List("Reviews", "Website", "Support url", "Support email", "Metacritic url", "Score rank", "Notes", "Tags", "Screenshots", "Movies").contains(column))
val refinedDF = df.select(columnsToKeep.map(df.col): _*)

refinedDF.printSchema()
refinedDF.show()


refinedDF.write.option("header","true").csv("refined_data.csv")

spark.stop()

