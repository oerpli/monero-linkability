package main.scala

import java.io.PrintWriter
import java.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.Row

import scala.collection.mutable

object Main {

  // https://stackoverflow.com/questions/30304810/dataframe-ified-zipwithindex, second answer
  def dfZipWithIndex(df: DataFrame,
                     offset: Int = 1,
                     colName: String = "id",
                     inFront: Boolean = true): DataFrame = {
    df.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map(
        ln =>
          Row.fromSeq(
            (if (inFront) Seq(ln._2 + offset) else Seq())
              ++ ln._1.toSeq ++
              (if (inFront) Seq() else Seq(ln._2 + offset))
        )),
      StructType(
        (if (inFront) Array(StructField(colName, LongType, false))
         else Array[StructField]())
          ++ df.schema.fields ++
          (if (inFront) Array[StructField]()
           else Array(StructField(colName, LongType, false)))
      )
    )
  }
  def main(args: Array[String]) {

    val spark = org.apache.spark.sql.SparkSession.builder
      .appName("Monero Linkability v2")
      .getOrCreate

    // used for SQLSpark API (DataFrame)
    import spark.implicits._

    try {
      val bucket_name = args(0)
      val input_fn = args(1)

      // Load the lines of text
      val lines = spark.read
        .format("csv")
        .option("header", "true")
        .load("gs://" + bucket_name + "/" + input_fn)
        .toDF("key_image", "coin_candidate")

      //		optimization: numbers as id instead of string hashes
      val key_image_id = dfZipWithIndex(lines.select("key_image").distinct(),
                                        colName = "key_image_id")
        .withColumnRenamed("key_image", "key_image_join")
      val candidate_id =
        dfZipWithIndex(lines.select("coin_candidate").distinct(),
                       colName = "coin_candidate_id")
          .withColumnRenamed("coin_candidate", "coin_candidate_join")

      // (key_image, candidate1)
      // (key_image, candidate2)
      val key_image_candidate = lines
        .join(key_image_id, $"key_image" === $"key_image_join")
        .join(candidate_id, $"coin_candidate" === $"coin_candidate_join")
        .select("key_image_id", "coin_candidate_id")

      val tx_input = key_image_candidate
        .map(row => (row(0).asInstanceOf[Long], row(1).asInstanceOf[Long]))
        .rdd

      // (key_image, [candidate1, candidate2, ...])
      val tx_inputs = Array(
        tx_input
          .groupByKey()
          .mapValues(iterable => mutable.Set(iterable.toSeq: _*))
          .collectAsMap()
          .toSeq: _*)

      //   val tx_inputs_map = tx_inputs
      //     .map(a => (a._1, mutable.Set(a._2.toSeq: _*)))
      //     .toSeq
      //     .sortWith(_._1 < _._1)
      val input_txs = tx_inputs
        .map {
          case (tx, inputs) => {
            inputs.toSeq.map(input => (input, tx))
          }
        }
        .flatten
        .groupBy(_._1)
        .map(a => (a._1, mutable.Set(a._2.map(_._2).toSeq: _*)))

      var numOfIterations = 0
      var keysToCheck = mutable.Map(tx_inputs.filter(_._2.size == 1).toSeq: _*)

      while (keysToCheck.size > 0) {
        numOfIterations += 1
        var changed = mutable.Map.empty[Long, mutable.Set[Long]] // cache elements that only have one remaining
        val keys_iterator = keysToCheck.iterator
        for ((tx_matched, values) <- keysToCheck) { // match transactions with only one input
          val input = values.min // only one element in Set, so just get any element
          // Remove inputs used in matched transactions from other Txs
          input_txs(input)
            .filter(tx_matched != _)
            .foreach(tx => {
              val (_, txinputs) = tx_inputs(tx.asInstanceOf[Int]) // Java only supports Int indexes
              txinputs.remove(input)
              // after removing inputs check if only 1 remains, if yes, add to set changed in next iteration
              if (txinputs.size == 1) {
                changed.put(tx, txinputs)
                // changed += ((tx, txinputs)) // need 2 brackets bc of compiler "bug"
                // https://stackoverflow.com/questions/26606986/scala-add-a-tuple-to-listbuffer?answertab=votes#tab-top
              }
            })
        }
        keysToCheck = changed
      }

      val tx_realInput =
        tx_inputs.filter(_._2.size == 1).map(a => (a._1, a._2.min))
      val percentage = tx_realInput.size * 1.0 / tx_inputs.size

      //convert Long back to String
      val tx_realInput_rdd = spark.sparkContext.parallelize(tx_realInput.toSeq)
      val key_image_coin_determined =
        tx_realInput_rdd.toDF("image_id", "determined_coin_id")
      val determined_coins = key_image_coin_determined
        .join(key_image_id, $"image_id" === $"key_image_id", joinType = "inner")
        .join(candidate_id,
              $"determined_coin_id" === $"coin_candidate_id",
              joinType = "inner")
        .select("key_image_join", "coin_candidate_join")

      // results are in two columns: key_image_join, coin_candidate_join

      // here we are printing whole dataframe just to display the result
      // consequence is that it is all collected to the driver and then printed
      // WARNING: you don't want to do this in production!!
      determined_coins.collect().foreach(println)

      // in production, you should save it in HDFS / Hive / Cloud ...
      // e.g. determined_coins.write.option("header", "true").csv("result.csv")
      // be aware that you need to set up additional configuration so that you can connect to your storage space

      new PrintWriter(System.out) {
        write("Percentage of determined real coins: " + percentage + "\n")
        write("Number of iterations: " + numOfIterations + "\n")
        close()
      }
    } finally {
      spark.stop()
    }
  }

}
