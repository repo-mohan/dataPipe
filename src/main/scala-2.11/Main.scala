/**
  * Created by Mohan on 7/18/2017.
  */
package org.mohan.spark.sql.etl

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import net.jcazevedo.moultingyaml._
import net.jcazevedo.moultingyaml.DefaultYamlProtocol._
import scala.io.Source

case class Extract(eSource: String, eType: String, eStgTableName: String)

case class Transform(tSQL: String, tStgTableName: String)

case class Load(lTargetName: String, lStgTableName: String)

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

    ExtractSources(spark, appConfig)

    spark.stop()
  }

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
  private def ExtractSources(spark: SparkSession , appConfig: etlConfig): Unit = {
    appConfig.Extracts.foreach{obj_Extract =>

      obj_Extract.eType match {
        case "JSON" =>
          val df = spark.read.json(obj_Extract.eSource)
          df.createOrReplaceGlobalTempView(obj_Extract.eStgTableName)
          print(obj_Extract.eStgTableName + " counts: " + df.count())
      }
    }
  }

  private def TransformSqls(spark: SparkSession, appConfig: etlConfig): Unit = {
    println("process the sqls")
  }
  private def WriteOutput(spark: SparkSession): Unit = {
    println("writing outputs")
  }
}
