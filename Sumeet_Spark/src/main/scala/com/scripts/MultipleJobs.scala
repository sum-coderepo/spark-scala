import java.util.concurrent.{Callable, Executors}

import org.apache.spark.{SparkContext, SparkConf}

object MultipleJobs {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("Multiple Jobs")
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.split.minsize", "2000000000")

    val executorService = Executors.newFixedThreadPool(2)
    // Start thread 1
    val future1 = executorService.submit(new Callable[Long]() {
      @Override
      def call: Long = {
        val DATA_PATH = "/home/hadoop/input/data_wide_3.txt"
        val rdd = sc.textFile(DATA_PATH)
        return rdd.count()
      }
    })
    // Start thread 2
    val future2 = executorService.submit(new Callable[Long]() {
      @Override
      def call: Long = {
        val DATA_PATH = "/home/hadoop/input/data_wide_4.txt"
        val rdd = sc.textFile(DATA_PATH)
        return rdd.count()
      }
    })
    // Wait thread 1
    println("File 1 Count:" + future1.get())
    // Wait thread 2
    println("File 2 Count" + future2.get())

    sc.stop()
  }
}