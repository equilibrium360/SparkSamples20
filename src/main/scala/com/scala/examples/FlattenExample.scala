package com.scala.examples

/**
  * Created by UNIVERSE on 12/5/16.
  */
object FlattenExample {

  def main(arg: Array[String]): Unit ={

    val list= List(1,2,3,4,5,6)
    def g(v:Int) = List(v-1, v, v+1)
    list.map(x => g(x)).foreach(println(_))
    list.flatMap(x => g(x)).foreach(println(_))


  }

}
