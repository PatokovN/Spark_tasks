import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ DateType, DoubleType, IntegerType, StringType, StructField, StructType }

object MostPopularProduct extends App {

  val spark = SparkSession.builder().appName("Most_popular_product").config("spark.master", "local").getOrCreate()

  val customerSchema = StructType(
    Array(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("email", StringType),
      StructField("joinDate", DateType),
      StructField("status", StringType)
    )
  )

  val productSchema = StructType(
    Array(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("price", DoubleType),
      StructField("numberOfProducts", IntegerType)
    )
  )

  val orderSchema = StructType(
    Array(
      StructField("customerID", IntegerType),
      StructField("orderID", IntegerType),
      StructField("productID", IntegerType),
      StructField("numberOfProduct", IntegerType),
      StructField("orderDate", DateType),
      StructField("status", StringType)
    )
  )
  val customerDF = spark.read
    .format("csv")
    .option("dateFormat", "YYYY-MM-DD")
    .option("sep", "\t")
    .schema(customerSchema)
    .option("path", "src/main/resources/customer.csv")
    .load()

  val productDF = spark.read
    .format("csv")
    .option("sep", "\t")
    .schema(productSchema)
    .option("path", "src/main/resources/product.csv")
    .load()

  val orderDF = spark.read
    .format("csv")
    .option("dateFormat", "YYYY-MM-DD")
    .option("sep", "\t")
    .schema(orderSchema)
    .option("path", "src/main/resources/order.csv")
    .option("sep", "\t")
    .load()

  private val meaningInfoDF = getMeaningTable(customerDF, orderDF, productDF)

  private val sumOfProductForCustomerDF: DataFrame = sumOfProduct(meaningInfoDF)

  private val numberOfFavoriteProduct = mostPopularProducts(sumOfProductForCustomerDF)

  private val result = getResultTable(sumOfProductForCustomerDF, numberOfFavoriteProduct)

  result.write
    .format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .save("src/main/resources/favorite_product.csv")

  def getMeaningTable(customerDF: DataFrame, orderDF: DataFrame, productDF: DataFrame): DataFrame = {
    val customerToOrderDF =
      orderDF
        .join(
          customerDF.withColumnRenamed("name", "customer_name"),
          customerDF.col("id") === orderDF.col("customerId"),
          "inner"
        )
        .where(
          (customerDF.col("status") === "active").and(orderDF.col("status") =!= "canceled")
        )

    val customerOrderProductDF =
      customerToOrderDF.join(
        productDF.withColumnRenamed("name", "product_name"),
        productDF.col("id") === customerToOrderDF.col("productId"),
        "inner"
      )

    customerOrderProductDF.select(
      col("customer_name"),
      col("product_name"),
      col("numberOfProduct")
    )
  }
  def sumOfProduct(meaningDF: DataFrame): DataFrame =
    meaningDF
      .groupBy(col("product_name"), col("customer_name"))
      .sum("numberOfProduct")
      .withColumnRenamed("sum(numberOfProduct)", "sum")

  def mostPopularProducts(totalProductForCustomerDF: DataFrame): DataFrame =
    totalProductForCustomerDF.groupBy(col("customer_name")).max("sum")

  def getResultTable(sumProductTable: DataFrame, mostPopularProductTable: DataFrame): DataFrame = {
    val resultJoinCondition = (sumProductTable.col("sum") ===
      mostPopularProductTable.col("max(sum)")).and(
      sumProductTable.col("customer_name") ===
        mostPopularProductTable.col("customer_name")
    )

    sumProductTable
      .join(mostPopularProductTable, resultJoinCondition, "left_semi")
      .select(col("customer_name"), col("product_name"))
  }

}
