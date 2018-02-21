package com.uttam.xml

import org.apache.spark.sql.{SparkSession,Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf


/**
  * Created by UNIVERSE on 11/10/16.
  */
object UDFSample {

  def main(args : Array[String]) = {

    val ss = SparkSession.builder()
      .appName("UDAF in Spark")
      .master("local[*]")
      .getOrCreate()

    val ratingSchema = StructType(Array(
      StructField("userid", IntegerType, true),
      StructField("itemid", StringType, true),
      StructField("rating", StringType, true),
      StructField("timestamp", StringType, true)))


    //Define Schema for UDF Ouput
    val newNestedSch = ArrayType(StructType(Array(
      StructField("chapterName", StringType, true),

      StructField("pagecount", StringType, true))))



    val ratingDF = ss.read
      .format("com.databricks.spark.csv")
      .option("header", "false") // Use first line of all files as header
      .option("delimiter", "\t")
      .schema(ratingSchema)
      .load("datafiles/movie/ratings.txt")

    def getItemCode(curCode: String):String ={

      curCode+" Update String"

    }

    import org.apache.spark.sql.functions.udf
    val getItemCodeUDF = udf((curCode: String) => {

      curCode + "Update String"
    })


    ratingDF.withColumn("NewCol", getItemCodeUDF(ratingDF("itemid")) ).show()


    //Another way of adding udf as regular function
    val itemlbl = udf(getItemIdLabel(_:String))

    ratingDF.withColumn("ItemLabel", itemlbl(ratingDF("itemid")) ).show()



    //Specify Schema for UDF output

    val nestedUDF = udf(createNestedRow(_:String), newNestedSch)
   val nestDF =  ratingDF.withColumn("NewNestedCol", nestedUDF(ratingDF("itemid") ) )

    nestDF.printSchema()
    nestDF.show()







    //ratingDF.withColumn("NewTinLabel", tinCheckUDF(ratingDF("tin")) ).show()








  }

  // regular spark function
  def getItemIdLabel(curId: String):String = {

    return "ItemLabel"

  }


  //Add a Column to DataFrame with Complex Data Type
  def createNestedRow(colval: String):Seq[Row] ={

    Seq(Row("Chapter1", "20 Pages"),  Row("Chapter2", "40 Pages"))
  }






}
