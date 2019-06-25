package com.ruozedata.LogETL.spark


import com.ruozedata.LogETL.Utils.{ComputeTasks, FileUtil}
import com.ruozedata.bigdata.spark.EmpParser
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SaveMode, SparkSession}

object LogETLApp extends Logging {

  def main (args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName( "LogETLApp" ).master( "local[2]" )
      .getOrCreate()
    val conf = spark.sparkContext.hadoopConfiguration
    //    val time = spark.sparkContext.getConf.get("spark.time","")
    val time = new StringBuffer( "2019060815" )
    val applicationName = spark.sparkContext.getConf.get( "spark.app.name" )
    val fileSystem = FileSystem.get( conf )


    if (StringUtils.isEmpty( time )) {
      logError( "spark.time is required" )
      System.exit( 0 )
    }

    try {
      val startTime = System.currentTimeMillis()
      val day = time.substring( 0, 8 )
      val hour = time.substring( 8, 10 )
      val input = s"hdfs://node1:8020/ruozedata/offline/emp/raw/$time/"
      val output = s"hdfs://node1:8020/ruozedata/offline/emp/col/$time"
      val resultDir = "hdfs://node1:8020/ruozedata/offline/emp/col/"

      val totals = spark.sparkContext.longAccumulator( "totals" )
      val errors = spark.sparkContext.longAccumulator( "errors" )

      var logDF = spark.read.text( input ).coalesce( ComputeTasks.getcoalesces( conf, input, 200 ) )
      logDF = spark.createDataFrame( logDF.rdd.map( x => {
        EmpParser.parseLog( x.getString( 0 ), totals, errors )
      } ).filter( _.length != 1 ), EmpParser.struct )

      logDF.write.format( "parquet" ).option( "compression", "none" ).partitionBy( "day", "hour" )
        .mode( SaveMode.Overwrite ).save( output )

      val dayDir = fileSystem.listStatus( new Path( output ) )
      for (day <- dayDir) {
        val hourDir = fileSystem.listStatus( new Path( day.getPath.toString ) )
        for (hour <- hourDir) {
          val parquetFile = fileSystem.listFiles( new Path( hour.getPath.toString ), true )
          var num = 1
          while (parquetFile.hasNext) {

            val nowFileName = parquetFile.next.getPath.toString
            val newFileName = nowFileName.replace( "hdfs://", "" ).split( "/" )

            if (newFileName.size > 8) {
              val day = newFileName( 6 )
              val hour = newFileName( 7 )
              val rightInput = resultDir + day + "/" + hour + "/" + time + "-" + num + ".parquet"
              val rightPathDir = resultDir + day + "/" + hour + "/"
              if (FileUtil.exists( conf, rightPathDir )) {
                fileSystem.mkdirs( new Path( rightPathDir ) )
              }

              fileSystem.delete( new Path( rightInput ), true )
              fileSystem.rename( new Path( nowFileName ), new Path( rightInput ) )
            }
            num += 1
          }
        }
      }

      fileSystem.delete( new Path( output ), true )

      val totalsValue = totals.count
      val errorsValue = errors.count
      val executeTime = (System.currentTimeMillis() - startTime) / 1000
      logError( s"$applicationName,作业执行成功，执行时间：$executeTime,总记录数：$totalsValue,错误记录数：$errorsValue" )


    } catch {
      case e: Exception => logError( "$applicationName,作业执行失败" )

    } finally {


      if (spark != null) {
        spark.stop()
      }
    }


  }

}
