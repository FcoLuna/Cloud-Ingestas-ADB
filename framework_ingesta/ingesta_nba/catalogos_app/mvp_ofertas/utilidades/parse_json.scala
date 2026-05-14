// Databricks notebook source
import scala.io.Source
import org.apache.spark.sql.functions._

// COMMAND ----------

object parse_json {
  def obtenerValorJSON(path_json: String, id_json_padre: String, id_json_hijo: String) : String = {

      val df = spark.read.option("multiline",true).json(path_json)

      val valor_sal = df.select(explode(col(id_json_padre)).alias("parent")).
        selectExpr(s"parent.$id_json_hijo as hijo").
        where(col("hijo").isNotNull).
        as[String].
        collect().
        headOption.getOrElse("")

      valor_sal
  }
}

// COMMAND ----------

/*
import org.apache.spark.sql.functions._

object parse_json {
  def obtenerValorJSON(path_json: String, id_json_padre: String, id_json_hijo: String) : String = {

    val df = spark.read.option("multiline", true).json(path_json)

    // Accede directamente al campo dentro del STRUCT
    val valor_sal = df.select(s"$id_json_padre.$id_json_hijo as hijo")
      .where(col("hijo").isNotNull)
      .as[String]
      .collect()
      .headOption
      .getOrElse("")

    valor_sal
  }
}
*/
