package com.gravity

import com.typesafe.config.ConfigFactory

/**
  * Created by MASTER on 2/20/2018.
  */
class ExportTables {

  def getTablesList()= {
    val tabList = ConfigFactory.load("export_tables.conf").getList("exportTables")


    // println("seqComp "+ jobConfig.getConfig("seqComp"))
    //  val seqComp = jobConfig.getConfig("seqComp")
    println("tabList "+ tabList)
    /*   val clist = jobConfig.getConfigList ("seqComp")
      println("size "+  clist.size())
       clist.get(0).getList()*/
  }
  def main(args: Array[String]) {

  }



}
