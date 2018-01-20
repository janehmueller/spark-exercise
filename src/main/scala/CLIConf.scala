import org.rogach.scallop.ScallopConf

class CLIConf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val path = opt[String](descr = "The path to the folder of the TPCH data.")
    val cores = opt[Int](descr = "The number of local cores Spark should use.")
    verify()
}
