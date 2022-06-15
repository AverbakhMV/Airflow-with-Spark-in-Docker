// Get data from Oracle table, transform it and load to Greenplum

package ru.filit.connection_test

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import scala.sys.process.Process

object Main  {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("spark-sql-labs")
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val df = load(spark)
    val newDF = transform(df)
    save(newDF)
  }


  /*
  * load data from oracle table to dataframe
  *@param spark: SparkSession
  * @return DataFrame
   */
  def load(spark: SparkSession): DataFrame = {
    spark.read.format("jdbc")
      .option("url", oracle_url)
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .option("dbtable", "ma_test")
      .option("user", oracle_user)
      .option("password", oracle_password)
      .load()
  }

  /*
  * udf to get user name/login from e-mail
  * @param email: String
  * @return string, cutting everything starting from @
   */
  val getName = udf((email: String) => email.split('@')(0))

  /*
  *calculate total number of different products bought by each customer and
  * replace customer email with his/her name/login
  * @param DataFrame
  * @return DataFrame
   */
  def transform(df: DataFrame): DataFrame = {
    df.groupBy("email", "product_name")
      .agg(sum("number_of_product") alias("total"))
      .withColumn("name", getName(col("email")))
      .select("name", "product_name", "total")
      .orderBy("name", "product_name")
  }

  /*
  * save resulting dataframe to Greenplum table
  * @param DataFrame
  * @return None
   */
  def save(df: DataFrame): Unit = {
    df.write.format("jdbc")
      .mode("overwrite")
      .option("url", psg_url)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", "public.ma_test")
      .option("user", psg_user)
      .option("password", psg_password)
      .save()
  }
}
