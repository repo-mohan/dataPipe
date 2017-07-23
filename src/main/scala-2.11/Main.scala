/**
  * Created by Mohan on 7/18/2017.
  */
package org.mohan.spark.sql.etl

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import net.jcazevedo.moultingyaml._
import net.jcazevedo.moultingyaml.DefaultYamlProtocol._

import scala.io.Source

case class Extract(eSource: String, eType: String, eStgTableName: String)

case class Transform(tSQL: String, tStgTableName: String)

case class Load(lTargetName: String, tStgTableName: String)

case class etlConfig(AppName :String,
                     Extracts: List[Extract],
                     Transforms: List[Transform],
                     Loads: List[Load])
object Main {
  def main(args: Array[String]): Unit = {
    //reading config file
    // replace the hard coded value with args(0)
    val appConfig = fetchEtlConfig(args(0))


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
// This functions read the configuration file and retuns the contents in the form of a case class
  private def fetchEtlConfig(path: String):etlConfig  = {

    object MyYamlProtocol extends DefaultYamlProtocol {
      implicit val ExtractFormat = yamlFormat3(Extract)
      implicit val TransformFormat = yamlFormat2(Transform)
      implicit val LoadFormat = yamlFormat2(Load)
      implicit val etlConfigFormat = yamlFormat4(etlConfig)
    }

    import MyYamlProtocol._
    import net.jcazevedo.moultingyaml._

    val strYaml = Source.fromFile(path).mkString
    val astYaml = strYaml.parseYaml
    //println(astYaml.prettyPrint)
    val ccYaml  = astYaml.convertTo[etlConfig]
    ccYaml
    //println(ccYaml.getClass)
    //ccYaml.Extracts.foreach{println}
  }

  private def ExtractSources(spark: SparkSession , appConfig: etlConfig) :Unit = {
      val srcdf :List[DataFrame] = appConfig.Extracts.map(objExtract =>
        { val sdf = spark.read.json(objExtract.eSource)
          sdf.createOrReplaceTempView(objExtract.eStgTableName)
          sdf
        })
  }

  private def TransformSQLs(spark: SparkSession , appConfig: etlConfig) :Unit = {
    val tsqldf :List[DataFrame] = appConfig.Transforms.map(objTransform =>
    { val tdf = spark.sql(Source.fromFile(objTransform.tSQL).mkString)
      tdf.createOrReplaceTempView(objTransform.tStgTableName)
      //Loads Target Table
      LoadTargets(spark,tdf,objTransform.tStgTableName,appConfig)
      tdf
    })
    tsqldf
  }

  private def LoadTargets(spark: SparkSession,ldf: DataFrame,stgTabName: String, appConfig: etlConfig) :Unit = {
    appConfig.Loads.foreach(objLoad=> {
      if (stgTabName == objLoad.tStgTableName) {
        ldf.coalesce(1).write.format("csv").mode("overwrite").save(objLoad.lTargetName)
      }
    })
  }
}
