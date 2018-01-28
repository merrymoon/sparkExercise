package com.hey.spark.exercise

import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.log4j.{ Level, Logger }

/**
 *
 * Created by Yong He on 2018/01/18.
 * Copyright © ?  All Rights Reserved
 */

object bookingAgg {

  //Logger.getLogger("org").setLevel(Level.FATAL)

  @transient lazy val log = org.apache.log4j.LogManager.getLogger("myLogger")

  /*
   * function: Load data from CSV file
   */

  def loadCsv(spark: SparkSession, filename: String): DataFrame = {
    spark.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load(filename)
  }

  /*
   * Function: save dataframe to single CSV file
   */

  def saveDfToCsv(df: DataFrame, csvOutput: String,
                  sep: String = ",", header: Boolean = true): Unit = {

    df.coalesce(1).write //also can use repartition, but repartition will produce shuffling, the cost is high than coalesce.
      .format("com.databricks.spark.csv")
      .option("header", header.toString)
      .option("delimiter", sep)
      .mode("overwrite")
      .save(csvOutput)
  }

  /*
   * Function: Run Spark SQL
   */

  def runSQL(spark: SparkSession, sql: String): DataFrame = {
    spark.sql(sql)
  }

  def main(args: Array[String]): Unit = {

    /*
     * initialize variables
     */
    if (args.length == 0) {
      println("please sepcify parameter s3Bucket.")
      System.exit(1)
    }

    val s3Bucket = args(0)

    //need set AWS key into environment variable

    val s3AccessKeyID = sys.env.getOrElse("AWS_ACCESS_KEY_ID", "NULL") 
    val s3AccessSKeySecret = sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", "NULL")

    if (s3AccessKeyID == "NULL" || s3AccessSKeySecret == "NULL") {
      println("AWS secret key is invalid.")
      System.exit(1)
    }

    /*
     * initialize spark session
     */

    val spark = SparkSession
      .builder()
      .appName("SparkExerciseYong")
      //.master("local") //for local test
      //.config("spark.sql.shuffle.partitions", "3") //for local test
      //.config("spark.sql.autoBroadcastJoinThreshold", 104857600) //for local test
      .getOrCreate()

    val sc = spark.sparkContext
    sc.hadoopConfiguration.set("fs.s3.awsAccessKeyId", s3AccessKeyID)
    sc.hadoopConfiguration.set("fs.s3.awsSecretAccessKey", s3AccessSKeySecret)
    sc.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    /*
     * Reading CSV datasets from S3 bucket.
     */

    try {
      println("start reading data")

      val hotelsData_tmp = loadCsv(spark, s3Bucket + "/hotels.csv")
      val customersData_tmp = loadCsv(spark, s3Bucket + "/customers.csv")
      val hotelBookingsData_tmp = loadCsv(spark, s3Bucket + "/hotel_bookings.csv")

      // drop the columns which will not be used to make data smaller to cache table into memory
      val hotelsData = hotelsData_tmp.drop("HotelName").drop("RoomType").drop("StarRating").drop("Latitude").drop("Longitude")
      val customersData = customersData_tmp.drop("FirstName").drop("LastName").drop("Address")
      val hotelBookingsData = hotelBookingsData_tmp.drop("RoomRateUSD")

      hotelsData.createOrReplaceTempView("hotels")
      customersData.createOrReplaceTempView("customers")
      hotelBookingsData.createOrReplaceTempView("hotelBookings")

      println("data is loaded ")

    } catch {
      case error: Exception =>
        log.setLevel(Level.ERROR)
        log.error("while load data from s3 buckect,occuring issue with error:" + error.getMessage())
        System.exit(1)

    }

    /*
     * Aggregating data and write result datasets back  to S3 bucket
     *
     */
    try {
      //cache table, don't cache small table which use for join, if small table cached, it may not broadcast to Executor, will cause join performance degrade.
      //if table used only one time, not need cache table
      // parameter can reduce cached data size: spark.rdd.compress true
      var sql = "CACHE TABLE hotelBookings"
      runSQL(spark, sql)

      println("preparing aggregated data")
      /*
     * run sparkSQL to aggregate the datasets
     *
     * adjust spark.sql.shuffle.partitions(default 200) value for large data join. if your executor-cores less than 200, please decrease it to reduce task batches, otherwise performance will degrade for batches cost.
     * for join, small table use autoBroadcast to reduce shuffle   spark.sql.autoBroadcastJoinThreshold(default 10M)
		*/

      //bookingStayInterval
      sql =
        s"""
         |SELECT  c.Gender,floor(datediff(current_date(),c.BirthDate) / 365) AS age,c.County
         |,SUM(datediff(TO_DATE(CAST(UNIX_TIMESTAMP(StayDate, 'dd.mm.yy') AS TIMESTAMP)),TO_DATE(CAST(UNIX_TIMESTAMP(BookingDate, 'dd.mm.yy') AS TIMESTAMP)) ) ) AS Interval_SUM
         |,AVG(datediff(TO_DATE(CAST(UNIX_TIMESTAMP(StayDate, 'dd.mm.yy') AS TIMESTAMP)),TO_DATE(CAST(UNIX_TIMESTAMP(BookingDate, 'dd.mm.yy') AS TIMESTAMP)) ) ) AS Interval_AVG
         |FROM hotelBookings hb JOIN customers c ON c.ClientID=hb.ClientID
         |GROUP BY c.Gender,floor(datediff(current_date(),c.BirthDate) / 365),c.County
       """.stripMargin
      val bookingStayInterval = runSQL(spark, sql)

      //stayLengthByCityCounty
      sql =
        s"""
         |SELECT  c.city,c.County,SUM(hb.StayDuration) AS StayDuration
         |FROM hotelBookings hb JOIN customers c ON c.ClientID=hb.ClientID
         |GROUP BY c.city,c.County         
        """.stripMargin
      val stayLengthByCityCounty = runSQL(spark, sql)

      //stayLenthByAgeGender
      sql =
        s"""
         |SELECT  c.Gender,floor(datediff(current_date(),c.BirthDate) / 365) AS age,SUM(hb.StayDuration) AS StayDuration
         |FROM hotelBookings hb JOIN customers c ON c.ClientID=hb.ClientID
         |GROUP BY c.Gender,floor(datediff(current_date(),c.BirthDate) / 365)       
        """.stripMargin
      val stayLenthByAgeGender = runSQL(spark, sql)
      println("data is aggregated. ")

      try {
        println("Writing aggregated data back to s3 bucket...")

        saveDfToCsv(bookingStayInterval, s3Bucket + "/bookingStayInterval", ",")
        saveDfToCsv(stayLengthByCityCounty, s3Bucket + "/stayLengthByCityCounty", ",")
        saveDfToCsv(stayLenthByAgeGender, s3Bucket + "/stayLenthByAgeGender", ",")

        println("write data to s3 bucket completed.")
      } catch {
        case error: Exception =>
          log.setLevel(Level.ERROR)
          log.error("while write result dataset to s3 buckect, occuring issue with error:" + error.getMessage())
          System.exit(1)

      }

    } catch {
      case error: Exception =>
        log.setLevel(Level.ERROR)
        log.error("Not able to aggregate data with error:" + error.getMessage())

    }

  }

}
