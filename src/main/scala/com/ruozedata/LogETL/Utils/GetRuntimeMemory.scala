package com.ruozedata.LogETL.Utils

object GetRuntimeMemory {

  def main (args: Array[String]): Unit = {
    println(Runtime.getRuntime.maxMemory)
  }

}
