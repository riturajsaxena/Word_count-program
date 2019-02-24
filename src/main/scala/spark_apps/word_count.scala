  package spark_apps


import org.apache.spark.sql.SparkSession
  import org.apache.log4j.Logger


object word_count {
    val logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Logger : Welcome to log4j")
    val inputfile1 = args(0)
    //val inputfile1 ="C:\\Hadoop\\README.txt"
    logger.info("Input file recieved sucessfully $inputfile1")

    val outputfile1 = args(1)
    println(inputfile1)
    println(outputfile1)
    //val outputfile1 ="C:\\Hadoop\\README_output.txt"
    logger.info("Input file recieved sucessfully outputfile1")

    val spark = SparkSession.builder().appName("word count").master("local[*]").getOrCreate()
    val input = spark.sparkContext.textFile(inputfile1)

    val words = input.flatMap(line => line.split(" "))

    val counts = words.map(words => (words, 1))
    val as = counts.reduceByKey(_ + _)
    //println(as)
    as.saveAsTextFile(outputfile1)
  }
}