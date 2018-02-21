package com.scala.examples

/**
  * Created by UNIVERSE on 12/14/16.
  */
object CollectionExample {

  def main(arg: Array[String]): Unit ={

    //appendItemToScalaSeq
    mergeSeqOfMaps

  }

  def appendItemToScalaSeq():Unit ={

    val s = Seq(1,2,3)
    val k = 44

    println(k +: s)


  }

  /* sssss
  *
  *
  *
  * */
  def mergeSeqOfMaps():Seq[Map[String,String]] = {

    val msL1 = Seq(Map("hello" -> "1.1", "world" -> "2.2"), Map("goodbye" -> "3.3", "hello" -> "4.4"))

    val msL2 = Seq(Map("hello" -> "33", "world" -> "33"), Map("goodbye" -> "44", "hello" -> "77"))

    val ms = msL1 ++ msL2

    ms.foreach(as => println(as))
    return ms

  }

  //Iterate over a sequence
  def iterateScalaSeq():Unit={

    val seqMaps = mergeSeqOfMaps()


  }


}
