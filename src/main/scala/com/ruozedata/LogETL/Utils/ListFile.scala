package com.ruozedata.LogETL.Utils

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.SparkSession

object ListFile {

  def main (args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ListFile").master("local[2]").getOrCreate()
    val conf = spark.sparkContext.hadoopConfiguration
    val fileSystem = FileSystem.get(conf)
    val time = "2019060815"
    val input = s"hdfs://node1:8020/ruozedata/offline/emp/col/$time/"



    val batchDir = fileSystem.listStatus(new Path(input))
    for(dir <- batchDir){
      val dir_dir = fileSystem.listStatus(new Path(dir.getPath.toString))
      for(dirs <- dir_dir){
            val batchFile = fileSystem.listFiles(new Path(dirs.getPath.toString),true)
            var num = 1
            while(batchFile.hasNext){

              val nowFileName = batchFile.next.getPath.toString
              val newFileName = nowFileName.replace("hdfs://","").split("/")
              if(newFileName.size > 8){
                val day = newFileName(6)
                val hour = newFileName(7)
                val rightInput = new Path(input.replace(s"$time","")) + day + "/" + hour + "/" + time + "-" + num +".parquet"
                println(rightInput)
              }
              num += 1
            }
      }
    }

//    val batchFile = fileSystem.listFiles(new Path(input),true)
//    while(batchFile.hasNext){
//      val nowFileName = batchFile.next.getPath.toString
//      val newFileName = nowFileName.replace("hdfs://","").split("/")
//
//      if(newFileName.size > 8){
//        val day = newFileName(6)
//        val hour = newFileName(7)
//        val rightInput = input + day + "/" + hour + "/" + time +".parquet"
//        println(rightInput)
//      }
//
//    }

  }

}
