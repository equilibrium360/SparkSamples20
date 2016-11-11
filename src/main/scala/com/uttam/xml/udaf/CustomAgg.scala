package com.uttam.xml.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import scala.collection.Seq

/**
  * Created by UNIVERSE on 11/10/16.
  */
class CustomAgg extends UserDefinedAggregateFunction {

//class CustomAgg {
  // Input Data Type Schema
  def inputSchema: StructType = StructType(Array(StructField("ratinginfo",StructType(Array(

                                  StructField("itemid", StringType, true),
                                  StructField("rating", StringType, true),
                                  StructField("timestamp", StringType, true))), true)))


  def bufferSchema:StructType = StructType(Array(StructField("ratingArray", ArrayType(StructType(Array(

    StructField("itemid", StringType, true),
    StructField("rating", StringType, true),
    StructField("timestamp", StringType, true))),true),true)))



  def dataType: DataType = ArrayType(StructType(Array(

    StructField("itemid", StringType, true),
    StructField("rating", StringType, true),
    StructField("timestamp", StringType, true))),true)


  // Self-explaining
  def deterministic = true


  // This function is called whenever key changes
  def initialize(buffer: MutableAggregationBuffer) = {


    buffer(0) = Seq.empty[Row]

  }

  // Iterate over each entry of a group
  def update(buffer: MutableAggregationBuffer, input: Row) = {

    val mainSeq = buffer.getSeq[Row](0)++:Seq(input.getStruct(0))
    buffer(0) =  mainSeq

  }

  // Merge two partial aggregates
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {

    val tmpPartialSeq = buffer2.getSeq[Row](0)
    val tmpSeq = buffer1.getSeq[Row](0)++:tmpPartialSeq
    buffer1(0) = tmpSeq

  }

  // Called after all the entries are exhausted.
  def evaluate(buffer: Row) = {

    val tp = buffer.getSeq[Row](0)
    buffer.getSeq[Row](0)
  }


















}
