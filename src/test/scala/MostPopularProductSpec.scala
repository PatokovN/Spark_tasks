import MostPopularProduct._
import org.apache.spark.sql.{ DataFrame, SparkSession }
import com.github.mrpowers.spark.fast.tests.{ DataFrameComparer, DatasetComparer, DatasetContentMismatch }
import org.scalatest.freespec.AnyFreeSpec

class MostPopularProductSpec extends AnyFreeSpec with DataFrameComparer with DatasetComparer {

  val sparkTest: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  private val customerOnlyActive = Seq(
    (1, "John", "john.cena@gmail.com", "2018-01-01", "active"),
    (2, "Liza", "john.cena@gmail.com", "2018-01-01", "active"),
    (3, "Bob", "john.cena@gmail.com", "2018-01-01", "active")
  )

  private val customerWithBanned = Seq(
    (1, "John", "john.cena@gmail.com", "2018-01-01", "active"),
    (2, "Liza", "john.cena@gmail.com", "2018-01-01", "active"),
    (3, "Bob", "john.cena@gmail.com", "2018-01-01", "banned")
  )

  private val orderAllDelivered = Seq(
    (1, 21, 1, 1000, "2018-02-23", "delivered"),
    (1, 21, 2, 500, "2018-02-23", "delivered"),
    (1, 21, 3, 1500, "2018-02-23", "delivered"),
    (2, 21, 1, 1000, "2018-02-23", "delivered"),
    (2, 21, 2, 1500, "2018-02-23", "delivered"),
    (2, 21, 3, 500, "2018-02-23", "delivered"),
    (3, 21, 1, 1500, "2018-02-23", "delivered"),
    (3, 21, 2, 500, "2018-02-23", "delivered"),
    (3, 21, 3, 1000, "2018-02-23", "delivered")
  )

  private val orderWithCanceled = Seq(
    (1, 21, 1, 1000, "2018-02-23", "delivered"),
    (1, 21, 2, 500, "2018-02-23", "delivered"),
    (1, 21, 3, 1500, "2018-02-23", "canceled"),
    (2, 21, 1, 1000, "2018-02-23", "delivered"),
    (2, 21, 2, 1500, "2018-02-23", "canceled"),
    (2, 21, 3, 500, "2018-02-23", "delivered"),
    (3, 21, 1, 1500, "2018-02-23", "canceled"),
    (3, 21, 2, 500, "2018-02-23", "delivered"),
    (3, 21, 3, 1000, "2018-02-23", "canceled")
  )

  private val orderWithDuplicateFavorProduct = Seq(
    (1, 21, 1, 1000, "2018-02-23", "delivered"),
    (1, 21, 2, 1500, "2018-02-23", "delivered"),
    (1, 21, 3, 1500, "2018-02-23", "delivered"),
    (2, 21, 1, 1000, "2018-02-23", "delivered"),
    (2, 21, 2, 1500, "2018-02-23", "delivered"),
    (2, 21, 3, 500, "2018-02-23", "delivered"),
    (3, 21, 1, 1500, "2018-02-23", "delivered"),
    (3, 21, 2, 1500, "2018-02-23", "delivered"),
    (3, 21, 3, 1000, "2018-02-23", "delivered")
  )

  private val products = Seq(
    (1, "Apple iPhone 7", 45990, 5000),
    (2, "MakBook Pro", 45990, 5000),
    (3, "MakBook Air", 45990, 5000)
  )
  import sparkTest.implicits._

  private val customerOnlyActiveTestDF = customerOnlyActive.toDF("id", "name", "email", "joinDate", "status")
  private val productTestDF = products.toDF("id", "name", "price", "numberOfProducts")
  private val orderAllDeliveredTestDF =
    orderAllDelivered.toDF("customerId", "orderId", "productId", "numberOfProduct", "orderDate", "status")
  private val expectedResultOkDF = Seq(
    ("John", "MakBook Air"),
    ("Liza", "MakBook Pro"),
    ("Bob", "Apple iPhone 7")
  ).toDF("customer_name", "product_name")

  private val customerWithBannedDF = customerWithBanned.toDF("id", "name", "email", "joinDate", "status")
  private val expectedResultBannedCustomerDF = Seq(
    ("John", "MakBook Air"),
    ("Liza", "MakBook Pro")
  ).toDF("customer_name", "product_name")

  private val orderWithCanceledDF =
    orderWithCanceled.toDF("customerId", "orderId", "productId", "numberOfProduct", "orderDate", "status")

  private val expectedResultCanceledOrdersDF = Seq(
    ("John", "Apple iPhone 7"),
    ("Liza", "Apple iPhone 7"),
    ("Bob", "MakBook Pro")
  ).toDF("customer_name", "product_name")

  private val orderDuplicateFavProductTestDF =
    orderWithDuplicateFavorProduct.toDF("customerId", "orderId", "productId", "numberOfProduct", "orderDate", "status")
  private val expectedResultDuplicateProductDF = Seq(
    ("John", "MakBook Air"),
    ("John", "MakBook Pro"),
    ("Liza", "MakBook Pro"),
    ("Bob", "Apple iPhone 7"),
    ("Bob", "MakBook Pro")
  ).toDF("customer_name", "product_name")

  "customer's status is active and there isn't canceled orders" in {
    val meaningInfoDF = getMeaningTable(customerOnlyActiveTestDF, orderAllDeliveredTestDF, productTestDF)

    val sumOfProductForCustomerDF: DataFrame = sumOfProduct(meaningInfoDF)

    val numberOfFavoriteProduct = mostPopularProducts(sumOfProductForCustomerDF)

    val result = getResultTable(sumOfProductForCustomerDF, numberOfFavoriteProduct)

    assertSmallDataFrameEquality(result, expectedResultOkDF, orderedComparison = false)
    assertLargeDataFrameEquality(result, expectedResultOkDF, orderedComparison = false)
  }

  "some customers have status 'banned' and there isn't canceled orders" in {
    val meaningInfoDF = getMeaningTable(customerWithBannedDF, orderAllDeliveredTestDF, productTestDF)

    val sumOfProductForCustomerDF: DataFrame = sumOfProduct(meaningInfoDF)

    val numberOfFavoriteProduct = mostPopularProducts(sumOfProductForCustomerDF)

    val result = getResultTable(sumOfProductForCustomerDF, numberOfFavoriteProduct)

    assertSmallDataFrameEquality(result, expectedResultBannedCustomerDF, orderedComparison = false)
    assertLargeDataFrameEquality(result, expectedResultBannedCustomerDF, orderedComparison = false)
  }

  "customer's status is active and there is canceled orders" in {
    val meaningInfoDF = getMeaningTable(customerOnlyActiveTestDF, orderWithCanceledDF, productTestDF)

    val sumOfProductForCustomerDF: DataFrame = sumOfProduct(meaningInfoDF)

    val numberOfFavoriteProduct = mostPopularProducts(sumOfProductForCustomerDF)

    val result = getResultTable(sumOfProductForCustomerDF, numberOfFavoriteProduct)

    assertSmallDataFrameEquality(result, expectedResultCanceledOrdersDF, orderedComparison = false)
    assertLargeDataFrameEquality(result, expectedResultCanceledOrdersDF, orderedComparison = false)
  }

  "customer's status is active, there isn't canceled orders and some customers have duplicate favorite product" in {
    val meaningInfoDF = getMeaningTable(customerOnlyActiveTestDF, orderDuplicateFavProductTestDF, productTestDF)

    val sumOfProductForCustomerDF: DataFrame = sumOfProduct(meaningInfoDF)

    val numberOfFavoriteProduct = mostPopularProducts(sumOfProductForCustomerDF)

    val result = getResultTable(sumOfProductForCustomerDF, numberOfFavoriteProduct)

    assertSmallDataFrameEquality(result, expectedResultDuplicateProductDF, orderedComparison = false)
    assertLargeDataFrameEquality(result, expectedResultDuplicateProductDF, orderedComparison = false)
  }

}
