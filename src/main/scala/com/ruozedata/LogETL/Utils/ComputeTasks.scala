package com.ruozedata.LogETL.Utils



import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


object ComputeTasks {

  def getcoalesces(conf:Configuration, path:String, size:Int)={

    val fileSystem = FileSystem.get(conf)
    var length = 0l
    fileSystem.globStatus(new Path(path)).map(x =>{
      length += x.getLen
    })
    (length/1024f/1024/size).toInt + 1
  }

  def main (args: Array[String]): Unit = {

    val conf = new Configuration()
    val path = "file:///Users/apple/Documents/project/LogETL/src/test/2.log"
    val tasks = ComputeTasks.getcoalesces(conf ,path,5)
    println(tasks)

  }

}
