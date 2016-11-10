package com.uttam.xml


import org.apache.spark
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._



/**
  * Created by UNIVERSE on 10/14/16.
  */




object XmlRW {

  case class NestedTest(h: Int)
  case class Test(i: Int, y: NestedTest)




  def main(args : Array[String]) = {

    val ss = SparkSession.builder()
      .appName("TestXML")
      .master("local")
      .getOrCreate()


    import ss.implicits._

    //Specifying Schema  is optional
    val customSchema = StructType(Array(
      StructField("_id", StringType, nullable = true),
      StructField("author", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("genre", StringType, nullable = true),
      StructField("price", DoubleType, nullable = true),
      StructField("publish_date", StringType, nullable = true),
      StructField("title", StringType, nullable = true),
    StructField("reviews", StructType(List(StructField("review", ArrayType(StringType), nullable=true))))))


    val xmlDF = ss.read.format("com.databricks.spark.xml")
      .option("rowTag", "book").load("datafiles/books.xml")

    xmlDF.printSchema()

    println(xmlDF.schema)
   val flatten =  xmlDF.toDF().select($"author", $"genre", $"title",
explode($"reviews.review").as("reviews_flat"))



    flatten.groupBy($"genre", $"title").agg(collect_list($"author")).show();
   //


   // flatten.map(r => Row(5, Row(88))).groupBy($"i").agg(collect_list($"y").as("ssss")).toDF().select($"i", explode($"ssss").as("ssss_flat")).show()

   // flatten.toDF().map[hgRow ](r => Row(44)).show()


   // flatten.groupBy($"genre", $"title").agg($"author").show();


    //flatten.map(r => Test(5, NestedTest(88))).groupByKey(tt => tt.i).reduceGroups((f,g) => f. ).toDF("one", "two").show()


   // xmlDF.groupByKey(r => r.get(1)).keys.show()


//Testing Group By Functionality






  }



}
