package com.uttam.xml


import org.apache.spark
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.util.LongAccumulator



/**
  * Created by UNIVERSE on 10/14/16.
  *
  * Read XML
  * use Explode
  * Use collec_list()
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

    //println(xmlDF.schema)

    // PASS complex datatypes to UDF Function and return  Complex DataTypes
    def getTinNode(node: Seq[Row]):String = {

      println("Inside function")

      println("Lengthhhh ")

   /*   node.map(r => { val ed =  r.getAs[Seq[Row]]("address")
                       val pr = ed.map(er => {
                         er.getAs("addid")

                         println("aaaa" + er.getAs("addid"))
                       })



        return "qqqqq"

                      })
                      */


      return "Done"

    }


    val tinCheckUDF = udf(getTinNode(_:Seq[Row]))

    xmlDF.withColumn("NewTin", tinCheckUDF(xmlDF("`tins`.`tin`"))).show()









    //Explode Function
   val flatten =  xmlDF.toDF().select($"author", $"genre", $"title",
explode($"reviews.review").as("reviews_flat"))



    //Perform aggregation on single column
    //To
    flatten.groupBy($"genre", $"title").agg(collect_list($"author")).show();






  }



}
