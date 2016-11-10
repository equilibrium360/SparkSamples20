package com.uttam.xml

import com.uttam.xml.TestGroupBy.Book
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by UNIVERSE on 11/1/16.
  */
object TestGroupBy {


  case class Book(title: String, author: String, rating: String, user: String, genre: String)


  case class Rate(rating:String, user:String)

  def main(args: Array[String]) = {

    val ss = SparkSession.builder()
      .appName("TestGroupBy")
      .master("local")
      .getOrCreate()

    val booklist = Seq(Book("Time Management", "david", "5", "donald", "selfhelp"),
      Book("Time Management", "david", "6", "hillary", "selfhelp"),
      Book("Time Management", "david", "9", "pense", "selfhelp"),
      Book("HarryPotter", "potter", "5", "obama", "children"),
      Book("HarryPotter", "potter", "5", "obama", "children"),
      Book("HarryPotter", "potter", "2", "bush", "children"),
      Book("HarryPotter", "potter", "6", "brad", "children"),
      Book("HarryPotter", "potter", "9", "Modi", "children"),
      Book("OrganizationSkills", "james", "5", "sarma", "selfhelp"),
      Book("OrganizationSkills", "james", "7", "prakash", "selfhelp"),
      Book("OrganizationSkills", "james", "8", "hiren", "selfhelp"),
      Book("OrganizationSkills", "james", "7", "samantha", "selfhelp"),
      Book("OrganizationSkills", "james", "9", "sankaran", "selfhelp"))




    val booklistdf = ss.createDataFrame(booklist)

   val booklistdf2  =  booklistdf.withColumn("rateObject", struct(booklistdf("rating"), booklistdf("user")))


    val booklistdf3 = booklistdf2.groupBy(booklistdf2("genre"), booklistdf2("author")).agg(collect_set("rateObject").as("rateList"))


    val booklistdf4 = booklistdf3.withColumn("authorRateObject",struct(booklistdf3("author"),booklistdf3("rateList") ))
    val booklistdf5 = booklistdf4.groupBy(booklistdf4("genre")).agg(collect_list("authorRateObject"))
    booklistdf5.foreach(println(_))


  }


}
