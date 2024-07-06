
    import org.apache.spark.sql.{SaveMode, SparkSession, functions}
    import org.apache.spark.sql.functions._

    import java.util.Properties

    // Starting a Spark session
    val spark = SparkSession.builder
      .appName("PreprocessAndSave")
      .config("spark.master", "local")
      .getOrCreate()

    // Importing the dataset
    val input_path = "D:/SEM - 5/DBMS/PROJECT/BDMS PROJECT/src/main/scala/movies.csv"
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

    // Finding attributes with null values
    val columnsWithNull = df.columns.filter(colName => df.filter(col(colName).isNull).count() > 0)
    columnsWithNull.foreach(println)

    // Removing samples with null values in specific attributes
    val columnsToCheck = Seq(
      "title", "rating", "genre", "released",
      "score", "votes", "director", "star",
      "country", "budget", "gross", "company", "runtime"
    )
    val cleanedDataNull = df.na.drop("any", columnsToCheck)

    // Checking for duplicate values
    val duplicateCount = cleanedDataNull.count() - df.dropDuplicates(df.columns).count()
    if (duplicateCount > 0) {
      println(s"Number of duplicate values: $duplicateCount")
    } else {
      println("No duplicate values found.")
    }

    // Removing entries with 0 in the "budget" or "gross" attributes
    val df2 = cleanedDataNull.filter(col("budget") =!= "0" && col("gross") =!= "0")

    // Number of samples in the cleaned data
    val numSamples = df2.count()
    println(s"Number of samples in cleaned data: $numSamples")

    // Remove commas from the "runtime" column
    val TransformedData = df2.withColumn("runtime", functions.regexp_replace(col("runtime"), ",", ""))

    // Displaying the first 10 entries of the processed data
    TransformedData.show(10, truncate = false)

    // Top 10 movies based on IMDB scores
    //val result1 = TransformedData
      //.select("title", "score")
      //.filter(expr("score >= 8.0"))
      //.orderBy(col("score").desc)
      //.limit(10)

    //result1.show()

    //val outputPath = "C:\\Users\\jaysa\\OneDrive\\Desktop\\results\\top_movies.csv"

    // Write the DataFrame to a CSV file
    //result1.write
      //.mode(SaveMode.Overwrite) // Use SaveMode.Overwrite to overwrite the file if it exists
      //.option("header", "true") // Include column headers in the output file
      //.csv(outputPath)

    // Top 10 genres based on IMDB scores
    //val result2 = TransformedData
    //.groupBy("genre")
    //.agg(avg("score").alias("genre_score"))
    //.orderBy(col("genre_score").desc)
    //.limit(10)

    //result2.show()


    // Stop the Spark session
    spark.stop()