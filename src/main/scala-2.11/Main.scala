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

    val eDfs:List[DataFrame] = ExtractSources(spark, appConfig)

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

  private def ExtractSources(spark: SparkSession , appConfig: etlConfig) :List[DataFrame] = {
      val df :List[DataFrame] = appConfig.Extracts.map(objExtract =>
        { val tdf = spark.read.json(objExtract.eSource)
          tdf.createOrReplaceTempView(objExtract.eStgTableName)
          tdf
        })
      //print("The count of data in first data Frame is " + df(0).printSchema())
      //print("The count of data in second data Frame is " + df(1).printSchema())
      val sqldf = spark.sql("select * from people_json")
      sqldf.collect().foreach(println)

    df
  }
}
