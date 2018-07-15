package org.mohan.spark.sql.etl

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import net.jcazevedo.moultingyaml._
import net.jcazevedo.moultingyaml.DefaultYamlProtocol._
import org.mohan.spark.sql.etl.Main.getClass
import com.databricks.spark.avro._
import org.apache.log4j.Logger
import org.apache.log4j.Level




import scala.io.Source

case class Extract(eSource: String, eSchema: String, erootTag: String, erowTag: String, eType: String, eStgTableName: String)
case class Transform(tSQL: String, tStgTableName: String)
case class Load(lTargetName: String, tStgTableName: String)
case class etlConfig(AppName :String,Extracts: List[Extract],Transforms: List[Transform],Loads: List[Load])

object Main {
  def main(args: Array[String]): Unit = {
 // what the hell is this
    //Disable Info logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    //reading config file
    // replace the hard coded value with args(0)

    val appConfig = FetchEtlConfig(args(0))


    val spark = SparkSession
      .builder()
      .appName(appConfig.AppName)
      .master("local[2]")
      .getOrCreate()


    import spark.implicits._
    //EXTRACT
    ExtractSources(spark, appConfig)
    //TRANSFORM and LOAD
    TransformSQLs(spark, appConfig)

    spark.stop()
  }

  private def FetchEtlConfig(path: String):etlConfig  = {
    // This functions read the configuration file and retuns the contents in the form of a case class
    object MyYamlProtocol extends DefaultYamlProtocol {
      implicit val ExtractFormat = yamlFormat6(Extract)
      implicit val TransformFormat = yamlFormat2(Transform)
      implicit val LoadFormat = yamlFormat2(Load)
      implicit val etlConfigFormat = yamlFormat4(etlConfig)
    }

    import MyYamlProtocol._
    import net.jcazevedo.moultingyaml._

    val strYaml = Source.fromInputStream(Source.getClass().getClassLoader().getResourceAsStream("etlConfig.yml")).mkString
    val astYaml = strYaml.parseYaml
    val ccYaml  = astYaml.convertTo[etlConfig]
    ccYaml

  }

  private def ExtractSources(spark: SparkSession , appConfig: etlConfig) :Unit = {
      val edf :List[DataFrame] = appConfig.Extracts.map(objExtract =>
        { objExtract.eType match {
          case "JSON" =>
            val sdf = spark.read.json (objExtract.eSource)
            sdf.createOrReplaceTempView (objExtract.eStgTableName)
            System.out.println(objExtract.eStgTableName)
            sdf.printSchema()
            sdf
          case "XML" =>
            val sdf = spark.read.format("com.databricks.spark.xml").option("rootTag", objExtract.erootTag).option("rowTag",objExtract.erowTag).load(objExtract.eSource)
            //val sdf = spark.read.format("com.databricks.spark.xml").load(objExtract.eSource)
            sdf.createOrReplaceTempView (objExtract.eStgTableName)
            //sdf.collect().map(println)
            System.out.println(objExtract.eStgTableName)
            sdf.printSchema()
            sdf
        }
        }
      )
  }
  private def TransformSQLs(spark: SparkSession , appConfig: etlConfig) :Unit = {
    val tsqldf :List[DataFrame] = appConfig.Transforms.map(objTransform =>
    { val tdf = spark.sql(Source.fromInputStream(Source.getClass().getClassLoader().getResourceAsStream(objTransform.tSQL)).mkString)
      tdf.createOrReplaceTempView(objTransform.tStgTableName)

      //Loads Target Table
      LoadTargets(spark,tdf,objTransform.tStgTableName,appConfig)
      tdf
    })
  }

  private def LoadTargets(spark: SparkSession,ldf: DataFrame,stgTabName: String, appConfig: etlConfig) :Unit = {
    appConfig.Loads.foreach(objLoad=> {
      if (stgTabName == objLoad.tStgTableName) {
        //coalesce is to merge the ouput files into 1 file.
        ldf.coalesce(1).write.format("csv").mode("overwrite").save(objLoad.lTargetName)
      }
    })
  }
}
