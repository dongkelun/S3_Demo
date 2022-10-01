package com.dkl.s3.spark

import org.apache.spark.sql.SparkSession

object SparkS3Demo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().
      master("local[*]").
      appName("SparkS3Demo").
      getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "access_key")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "secret_key")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "192.168.44.128:7480")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "false")

    testReadTxt(spark)
    testWriteAndReadParquet(spark)

    spark.stop()
  }

  /**
   * 测试Spark读S3txt
   * 需要先创建Bucket s3cmd mb s3://txt
   * 再上传test.txt s3cmd put test.txt s3://txt
   * 最后用Spark读
   */
  def testReadTxt(spark: SparkSession) = {
    val rdd = spark.read.text("s3a://txt/test.txt")
    println(rdd.count)
    rdd.foreach(println(_))
  }

  /**
   * s3cmd mb s3://test-s3-write
   * 需要先创建Bucket
   */
  def testWriteAndReadParquet(spark: SparkSession) = {
    import spark.implicits._

    val df = Seq((1, "a1", 10, 1000, "2022-09-27")).toDF("id", "name", "value", "ts", "dt")
    df.write.mode("overwrite").parquet("s3a://test-s3-write/test_df")

    spark.read.parquet("s3a://test-s3-write/test_df").show
  }
}
