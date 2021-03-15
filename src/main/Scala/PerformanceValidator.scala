import org.apache.spark.sql.SparkSession


object PerformanceValidator extends App {
  val spark = SparkSession.builder().appName("PerformanceValidator").master("local[*]")

    .config("spark.executor.cores", "2")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "3g")
    .getOrCreate()

  val sc = spark.sparkContext
   sc.setLogLevel("OFF")

  val inputParkingDF = spark.read.option("header",true)
    .option("inferSchema",true)
    .csv("E:\\Tutorials\\GKC\\Dataset\\NYC Parking Tickets")
    .coalesce(4)

  println(inputParkingDF.count())


  val groupedDF = inputParkingDF.groupBy("Issue Date").count()

  groupedDF.show()
  inputParkingDF.printSchema()
  inputParkingDF.show()

}
