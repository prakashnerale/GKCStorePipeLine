import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import com.typesafe.config.{Config, ConfigFactory}
import DataFunctionObj.read_schema
import java.time.LocalDate
import java.time.format.DateTimeFormatter




object DailyDataIngestAndRefine {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DailyDataIngestAndRefine").master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    //Reading dataFile from config File

    val DataConfig : Config= ConfigFactory.load("application.conf")
    val inputLocation = DataConfig.getString("path.inputLocation")
    val outputLocation = DataConfig.getString("path.outputLocation")


        //Reading Schema from Config
        val landingFileSchemaFromFile = DataConfig.getString("schema.landingFileSchema")
        val landingFileSchema =  read_schema(landingFileSchemaFromFile)

    /*val landingFileSchema = StructType(List(
      StructField("Sale_ID", StringType, true),
      StructField("Product_ID", StringType, true),
      StructField("Quantity_Sold", IntegerType, true),
      StructField("Vendor_ID", StringType, true),
      StructField("Sale_Date", TimestampType, true),
      StructField("Sale_Amount", DoubleType, true),
      StructField("Sale_Currency", StringType, true)))*/

      //handling dates(Input folder name which contains date)
    val dateToday = LocalDate.now()
    val dateYesterday = dateToday.minusDays(1)

    //describing the date in the format "_18072020"
    //val currDayZoneSuffix = "_" + dateToday.format(DateTimeFormatter.ofPattern("ddMMyyyy"))
    //val prevDayZoneSuffix = "_" + dateYesterday.format(DateTimeFormatter.ofPattern("ddMMyyyy"))
    val currDayZoneSuffix = "_18072020"
    val prevDayZoneSuffix = "_17072020"


    val landingFileDF = spark.read
      .schema(landingFileSchema)
      .option("delimiter", "|")
      .csv(inputLocation + "Sales_Landing/SalesDump" + currDayZoneSuffix)

    //landingFileDF.show()

    //landingFileDF.printSchema()

    // # Use Case 1 --->Removing null records from Quantity_Sold and Vendor_ID columns
    // finding valid data(have not null records)
    val validLandingData = landingFileDF.filter(col("Quantity_Sold").isNotNull
     && col("Vendor_ID").isNotNull)

    // validLandingData.show()
    // # Use Case 2-->
    //Saving Valid data in the location "Outputs/valid"
    validLandingData.write
      .mode("overwrite")
      .option("delimiter", "|")
      .option("header", true)
      .csv(outputLocation + "Valid/ValidData" +currDayZoneSuffix)



    val InValidLandingData = landingFileDF.filter(col("Quantity_Sold").isNull
      || col("Vendor_ID").isNull)

     //InValidLandingData.show()
    //Saving InValid data in the location "Outputs/Hold"

     InValidLandingData.write
      .mode("overwrite")
      .option("delimiter", "|")
      .option("header", true)
      .csv(outputLocation + "Hold/HoldData" +  currDayZoneSuffix)







  }


}
