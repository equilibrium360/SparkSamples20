package com.transform

import com.typesafe.config.ConfigFactory

/**
  * Created by MASTER on 2/9/2018.
  */
object JobController {

  def readJobConfig()= {
    val jobConfig = ConfigFactory.load().getConfig("jobConfig")


   // println("seqComp "+ jobConfig.getConfig("seqComp"))
  //  val seqComp = jobConfig.getConfig("seqComp")
    println("confList "+ jobConfig.getList("seqComp"))
 /*   val clist = jobConfig.getConfigList("seqComp")
   println("size "+  clist.size())
    clist.get(0).getList()*/
  }


  def main(ar:Array[String]):Unit = {
    readJobConfig()
  }

}
