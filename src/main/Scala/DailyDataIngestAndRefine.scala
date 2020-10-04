import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, when}
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

        val HoldFileSchemaFromFile = DataConfig.getString("schema.HoldFileSchema")
       val HoldFileSchema =  read_schema(HoldFileSchemaFromFile)

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
    val currDayZoneSuffix = "_19072020"
    val prevDayZoneSuffix = "_18072020"


    val landingFileDF = spark.read
      .schema(landingFileSchema)
      .option("delimiter", "|")
      .csv(inputLocation + "Sales_Landing/SalesDump" + currDayZoneSuffix)
    //landingFileDF.show()
    landingFileDF.createOrReplaceTempView("landingFileDF") //accessing DF by some name(for Sql Query)

    //landingFileDF.printSchema()

    // # Use Case 1 --->Removing null records from Quantity_Sold and Vendor_ID columns
    // finding valid data(have not null records)

   //validLandingData.show()
    // # Use Case 2-->
    //Saving Valid data in the location "Outputs/valid"
    /*validLandingData.write
      .mode("overwrite")
      .option("delimiter", "|")
      .option("header", true)
      .csv("E:\\Data\\ValidData")
*/


     //InValidLandingData.show()



    //Saving InValid data in the location "Outputs/Hold"

     /*InValidLandingData.write
      .mode("overwrite")
      .option("delimiter", "|")
      .option("header", true)
      .csv("E:\\Data\\HoldData")*/

    val validLandingData = landingFileDF.filter(col("Quantity_Sold").isNotNull
      && col("Vendor_ID").isNotNull)
    validLandingData.createOrReplaceTempView("validLandingData")



    //Checking whether updates were received on any previously hold data
    val previousHoldUpdateDF = spark.read
      .schema(HoldFileSchema)
      .option("delimiter", "|")
      .option("header",true)
      .csv(outputLocation + "Hold/HoldData" + prevDayZoneSuffix)
    //previousHoldUpdateDF.show()
    previousHoldUpdateDF.createOrReplaceTempView("previousHoldUpdateDF")//accessing DF by some name

    //Sql Query (for taking the updated records of next day data if its null taking previous day record)
    val UpdatedLandingData = spark.sql("select a.Sale_ID, a.Product_ID, " +
      "CASE " +
      "WHEN (a.Quantity_Sold IS NULL) THEN b.Quantity_Sold " +
      "ELSE a.Quantity_Sold " +
      "END AS Quantity_Sold, " +
      "CASE " +
      "WHEN (a.Vendor_ID IS NULL) THEN b.Vendor_ID " +
      "ELSE a.Vendor_ID " +
      "END AS Vendor_ID, " +
      "a.Sale_Date, a.Sale_Amount, a.Sale_Currency " +
      "from landingFileDF a left outer join previousHoldUpdateDF b on a.Sale_ID = b.Sale_ID ")

     previousHoldUpdateDF.createOrReplaceTempView("UpdatedLandingData")
     UpdatedLandingData.show()

    val releasedFromHold = spark.sql("Select VLD.Sale_ID " +
    "from validLandingData VLD INNER JOIN previousHoldUpdateDF PHD "+
    "ON VLD.Sale_ID = PHD.Sale_ID ")

    releasedFromHold.createOrReplaceTempView("releasedFromHold")

    val notReleasedFromHold = spark.sql("Select * from previousHoldUpdateDF "+
    "Where Sale_ID NOT IN (Select Sale_ID from releasedFromHold )")

    notReleasedFromHold.createOrReplaceTempView("notReleasedFromHold")

    val InValidLandingData = landingFileDF.filter(col("Quantity_Sold").isNull
      || col("Vendor_ID").isNull)
      .withColumn("Hold_Reason", when(col("Quantity_Sold").isNull, "Qty_Sold Missing")
      .otherwise(when(col("Vendor_ID").isNull, "Vendor_ID Missing")))
      .union(notReleasedFromHold)

    InValidLandingData.show()

    //InValidLandingData.createOrReplaceTempView("InValidLandingData")

    /*validLandingData.write
     .mode("overwrite")
     .option("delimiter", "|")
     .option("header", true)
     .csv("E:\\Data\\ValidData")


    //InValidLandingData.show()

    //Saving InValid data in the location "Outputs/Hold"

    InValidLandingData.write
     .mode("overwrite")
     .option("delimiter", "|")
     .option("header", true)
     .csv("E:\\Data\\HoldData")
*/
  }


}
