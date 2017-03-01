import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import com.databricks.spark.corenlp.functions._


object SenNLP {
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

    val processed = inputDF
      .select('sen, tokenize('sen).as('words), lemma('sen).as('lemma), pos('sen).as('pos), ner('sen).as('nerTags))
      .cache

    processed.write.json(outputDataset)

  } //main

}
