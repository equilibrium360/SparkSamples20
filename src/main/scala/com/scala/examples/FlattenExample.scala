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

    /*

    Following is output
    List(0, 1, 2)
List(1, 2, 3)
List(2, 3, 4)
List(3, 4, 5)
List(4, 5, 6)
List(5, 6, 7)
0
1
2
1
2
3
2
3
4
3
4
5
4
5
6
5
6
7
     */


  }

}
