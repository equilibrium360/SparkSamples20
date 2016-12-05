package com.uttam.xml

import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Random

/**
  * Created by UNIVERSE on 11/19/16.
  */
object SkewJoin {


  def main(args: Array[String]) = {



    val ss = SparkSession.builder()
      .appName("JoinData")
      .master("local[*]")
      .getOrCreate()



    val ratingSchema = StructType(Array(
      StructField("userid", IntegerType, true),
      StructField("itemid", StringType, true),
      StructField("rating", StringType, true),
      StructField("timestamp", StringType, true)))


    val ratingDF = ss.read
      .format("com.databricks.spark.csv")
      .option("header", "false") // Use first line of all files as header
      .option("delimiter", "\t")
      .schema(ratingSchema)
      .load("datafiles/movie/ratings.txt")

    ratingDF.count()

    println("ratingDF Count   "+ ratingDF.count())




    val userSchema = StructType(Array(
      StructField("uid", IntegerType, true),
      StructField("age", StringType, true),
      StructField("gender", StringType, true),
      StructField("occupation", StringType, true),
      StructField("zipcode", StringType, true)))


    val userDF = ss.read
      .format("com.databricks.spark.csv")
      .option("header", "false") // Use first line of all files as header
      .option("delimiter", "|")
      .schema(userSchema)
      .load("datafiles/movie/users.txt")


    //Find Skewed Data

    val ratingKeyCount = ratingDF.groupBy("userid").count().orderBy(desc("count"))

    val userKeyCount = userDF.groupBy("uid").count().orderBy(desc("count"))

    val userKeyCountProj = userKeyCount.select(userKeyCount("uid").as("userid"), userKeyCount("count").as("count"))


    val totalcount = ratingKeyCount.union(userKeyCountProj).groupBy("userid").agg(collect_list("count").as("listCount"))


    val prodCountUDF = udf(prodCountFun(_:Seq[Long]))

    val skewedKeys = totalcount.withColumn("recordCount", prodCountUDF(totalcount("listCount")) ).orderBy(desc("recordCount"))

    skewedKeys.show(10)

    //Found Skewed Data

    val skewedKeysList = skewedKeys.select("userid").takeAsList(10)




    //Remove only Skewed Data from Original Data
    //

    val skewKeysRecBroadcastVar = ss.sparkContext.broadcast[java.util.List[Row]](skewedKeysList)



    val skewKeysBCVal = skewKeysRecBroadcastVar.value

    val filterSkew: (Row => Boolean) = (arg: Row) => {
     // println("INput Row " + arg.mkString)
      skewKeysBCVal.contains(arg)
    }


    val filterSkewUDF = udf(filterSkew)

    val onlySkewedRatingDf = ratingDF.filter(filterSkewUDF( struct(ratingDF("userid")).as("aaa")) )

    println("filteredRatingDf  Count "+ onlySkewedRatingDf.count())


    val nonSkewedRatingDF = ratingDF.except(onlySkewedRatingDf)





    val getRandIntFun:(Int => Int)= (arg:Int) => {
      val randInt = new Random()
       (randInt.nextInt(5))
    }

    val getRandIntUDF = udf(getRandIntFun)



    val onlySkewRateDFKey =  onlySkewedRatingDf.withColumn("randomKey", getRandIntUDF(onlySkewedRatingDf("userid")))




    //val x = df.withColumn("dt", callUDF(dtFunc, DateType, col("dt_string")))



    onlySkewRateDFKey.show(50)


    val onlySkewedUserDf = userDF.filter(filterSkewUDF( struct(userDF("uid")).as("aaa")) )

    println("onlySkewedUserDf  Count "+ onlySkewedUserDf.count())


    val nonSkewedUserDF = userDF.except(onlySkewedUserDf)


    val onlySkewedUserDf0 = onlySkewedUserDf.withColumn("randomKey", lit(0))
    val onlySkewedUserDf1 = onlySkewedUserDf.withColumn("randomKey", lit(1))
    val onlySkewedUserDf2 = onlySkewedUserDf.withColumn("randomKey", lit(2))
    val onlySkewedUserDf3 = onlySkewedUserDf.withColumn("randomKey", lit(3))
    val onlySkewedUserDf4 = onlySkewedUserDf.withColumn("randomKey", lit(4))

   val onlySkewedUserDfAll = onlySkewedUserDf0.union(onlySkewedUserDf1).union(onlySkewedUserDf2).union(onlySkewedUserDf3).union(onlySkewedUserDf4)

    onlySkewedUserDfAll.show(20)

    onlySkewedUserDfAll.join(onlySkewRateDFKey, onlySkewRateDFKey("userid")===onlySkewedUserDfAll("uid") && onlySkewRateDFKey("randomKey")===onlySkewedUserDfAll("randomKey") ).show(1000)


































  }

  def prodCountFun(countList:Seq[Long]):Long = {
    if(countList.length > 1){

      return countList.reduceLeft(_*_)

    } else {
      return 0
    }
  }


}
