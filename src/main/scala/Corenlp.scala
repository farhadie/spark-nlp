import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.databricks.spark.corenlp.functions._


object Corenlp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Corenlp")
      // compress parquet datasets with snappy
      .config("spark.sql.parquet.compression.codec", "snappy")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val inputPath = args(0)
    val outputDataset = args(1)

    val inputDF = spark.read.json(inputPath)

    val sentences = inputDF
      .select('id, cleanxml('content).as('doc))
      .select('id, explode(ssplit('doc)).as('sen))

    sentences.write.csv(outputDataset)

  } //main
}
