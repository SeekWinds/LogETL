package com.ruozedata.LogETL.Utils

object Test {
  implicit def strToInt(str:String) = str.toInt

  def main (args: Array[String]): Unit = {

    println(add("100","10"))

  }

  def add(x:Int,y:Int) = x+y

}
