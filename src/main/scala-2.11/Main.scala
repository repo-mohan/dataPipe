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

object Main {
  case class Extract(eSource: String, eType: String)

  case class Transform(tSQL: String, tStgTableName: String)

  case class Load(lTargetName: String, lStgTableName: String)

  case class etlConfig(AppName :String,
                       Extracts: List[Extract],
                       Transforms: List[Transform],
                       Loads: List[Load])

  def main(args: Array[String]): Unit = {
    //reading config file
    // replace the hard coded value with args(0)
    fetchEtlConfig("C:\\HADOOP\\sparketl\\etlConfig.yml")

  /*  val spark = SparkSession
      .builder()
      .appName("sparketl")
      .master("local[2]")
      .getOrCreate()*/

 //   import spark.implicits._
 //   val df = spark.read.json("file:/C:/HADOOP/spark/examples/src/main/resources/people.json")
 //   df.write.parquet("file:/C:/HADOOP/spark/examples/src/main/resources/people.parquet")
 //   print(df.count())
 //   spark.stop()
  }

  private def fetchEtlConfig(path: String): Unit = {

    object MyYamlProtocol extends DefaultYamlProtocol {
      implicit val ExtractFormat = yamlFormat2(Extract)
      implicit val TransformFormat = yamlFormat2(Transform)
      implicit val LoadFormat = yamlFormat2(Load)
      implicit val etlConfigFormat = yamlFormat4(etlConfig)
    }

    import MyYamlProtocol._
    import net.jcazevedo.moultingyaml._

    val strYaml = Source.fromFile(path).mkString
    val astYaml = strYaml.parseYaml
    println(astYaml.prettyPrint)
    val ccYaml  = astYaml.convertTo[etlConfig]
    ccYaml.Extracts.foreach{println}
  }

  private def TransformSqls(spark: SparkSession): Unit = {
    println("process the sqls")
  }
  private def WriteOutput(spark: SparkSession): Unit = {
    println("writing outputs")
  }
}
