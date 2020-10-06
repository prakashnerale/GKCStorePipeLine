import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, when}
import com.typesafe.config.{Config, ConfigFactory}
import DataFunctionObj.read_schema
import java.time.LocalDate
import java.time.format.DateTimeFormatter



object EnrichProductReference {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("EnrichProductReference").master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val DataConfig : Config= ConfigFactory.load("application.conf")
    val inputLocation = DataConfig.getString("path.inputLocation")
    val outputLocation = DataConfig.getString("path.outputLocation")

    //Reading Valid data
    val validFileSchema  = StructType(List(
      StructField("Sale_ID", StringType, false),
      StructField("Product_ID", StringType, false),
      StructField("Quantity_Sold", IntegerType, false),
      StructField("Vendor_ID", StringType, false),
      StructField("Sale_Date", TimestampType, false),
      StructField("Sale_Amount", DoubleType, false),
      StructField("Sale_Currency", StringType, false)
    ))

    val productPriceReferenceSchema = StructType(List(
      StructField("Product_ID",StringType, true),
      StructField("Product_Name",StringType, true),
      StructField("Product_Price",IntegerType, true),
      StructField("Product_Price_Currency",StringType, true),
      StructField("Product_updated_date",TimestampType, true)
    ))
    val dateToday = LocalDate.now()
    val dateYesterday = dateToday.minusDays(1)

    //val currDayZoneSuffix  = "_" + dateToday.format(DateTimeFormatter.ofPattern("ddMMyyyy"))
    val currDayZoneSuffix  = "_18072020"
    //val prevDayZoneSuffix = "_" + dateYesterday.format(DateTimeFormatter.ofPattern("ddMMyyyy"))
    val prevDayZoneSuffix = "_17072020"

    val validDataDF = spark.read
      .schema(validFileSchema)
      .option("delimiter", "|")
      .option("header", true)
      .csv(outputLocation + "Valid/ValidData"+currDayZoneSuffix)
    validDataDF.createOrReplaceTempView("validData")


    //Reading Project Reference
    val productPriceReferenceDF = spark.read
      .schema(productPriceReferenceSchema)
      .option("delimiter", "|")
      .option("header", true)
      .csv(inputLocation + "Products")
    productPriceReferenceDF.createOrReplaceTempView("productPriceReferenceDF")

    val productEnrichedDF = spark.sql("select VD.Sale_ID, VD.Product_ID, PPR.Product_Name, "+
      "VD.Quantity_Sold, VD.Vendor_ID, VD.Sale_Date, " +
      "PPR.Product_Price * VD.Quantity_Sold  AS Sale_Amount,VD.Sale_Currency " +
    "FROM validData VD INNER JOIN productPriceReferenceDF PPR "+
    "ON VD.Product_ID = PPR.Product_ID ")

    productEnrichedDF.show()

    //productEnrichedDF.show()
    productEnrichedDF.write
      .option("header", true)
      .option("delimiter","|")
      .mode("overwrite")
      .csv(outputLocation + "Enriched/SaleAmountEnrichment/SaleAmountEnriched" + currDayZoneSuffix)
  }
}
