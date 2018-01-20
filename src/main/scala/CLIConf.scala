import org.rogach.scallop.ScallopConf

class CLIConf(arguments: Seq[String]) extends ScallopConf(arguments) {
    banner(
        """Usage: java -jar myJar.jar [OPTION]...
          |Options:
          |""".stripMargin)
    val path = opt[String](descr = "The path to the folder of the TPCH data.")
    val cores = opt[Int](descr = "The number of local cores Spark should use.")
    val outputFile = opt[String](descr = "Name of the output file.")
    verify()
}
