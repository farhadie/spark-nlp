import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import com.databricks.spark.corenlp.functions._


object Article2Sentence {
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
      .select('id, 'title, cleanxml('content).as('doc))
      .select('id, 'title, explode(ssplit('doc)).as('sen))
      .cache

    /*val processed = sentences
      .select(tokenize('sen).as('words), lemma('sen).as('lemma), ner('sen).as('nerTags), pos('sen).as('pos), sentiment('sen).as('sentiment))
      .cache
    */
    sentences.write.json(outputDataset)

  } //main
}
