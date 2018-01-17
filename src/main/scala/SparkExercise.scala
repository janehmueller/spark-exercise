import org.apache.spark.{SparkConf, SparkContext}

object SparkExercise {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("Spark Exercise")
            .setMaster("local[1]")
        val sc = SparkContext.getOrCreate(conf)
        val input = sc.parallelize(List.empty[String])
    }
}
