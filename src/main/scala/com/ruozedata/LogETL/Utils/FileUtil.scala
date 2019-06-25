package com.ruozedata.LogETL.Utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object FileUtil {

  def exists(conf:Configuration, path:String):Boolean = {
    val fs = FileSystem.get(conf)
    return fs.exists(new Path(path))
  }



}
