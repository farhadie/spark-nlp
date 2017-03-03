import com.databricks.spark.corenlp.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

/**
  * Created by sam on 3/3/17.
  */
object Pipline {
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
      .select('id, 'title, explode(ssplit('content)).as('sen))
      .cache

    val processed = sentences
      .select(tokenize('sen).as('words), lemma('sen).as('lemma), ner('sen).as('nerTags), pos('sen).as('pos))
      .cache

    processed.write.parquet(outputDataset)

  } //main
}
