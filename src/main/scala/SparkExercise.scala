import java.io.PrintWriter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkExercise {
    def main(args: Array[String]): Unit = {
        val conf = new CLIConf(args)
        val path = conf.path.getOrElse("./TPCH")
        val cores = conf.cores.getOrElse(4)
        val outputFile = conf.outputFile.getOrElse("inclusion_dependencies")
        val sparkJob = new SparkExercise(path, cores, outputFile)
        sparkJob.run()
    }
}

class SparkExercise(path: String, cores: Int, outputFile: String) extends Serializable {
    // input
    var customerCSV: RDD[(String, String, String, String, String, String, String, String)] = _
    var lineitemCSV: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)] = _
    var nationCSV: RDD[(String, String, String, String)] = _
    var ordersCSV: RDD[(String, String, String, String, String, String, String, String, String)] = _
    var partCSV: RDD[(String, String, String, String, String, String, String, String, String)] = _
    var regionCSV: RDD[(String, String, String)] = _
    var supplierCSV: RDD[(String, String, String, String, String, String, String)] = _

    // table schemata
    val customerSchema = List("C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_NATIONKEY", "C_PHONE", "C_ACCTBAL", "C_MKTSEGMENT", "C_COMMENT")
    val lineitemSchema = List("L_ORDERKEY", "L_PARTKEY", "L_SUPPKEY", "L_LINENUMBER", "L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_TAX", "L_RETURNFLAG", "L_LINESTATUS", "L_SHIP", "L_COMMIT", "L_RECEIPT", "L_SHIPINSTRUCT", "L_SHIPMODE", "L_COMMENT")
    val nationSchema = List("N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT")
    val ordersSchema = List("O_ORDERKEY", "O_CUSTKEY", "O_ORDERSTATUS", "O_TOTALPRICE", "O_ORDER", "O_ORDERPRIORITY", "O_CLERK", "O_SHIPPRIORITY", "O_COMMENT")
    val partSchema = List("P_PARTKEY", "P_NAME", "P_MFGR", "P_BRAND", "P_TYPE", "P_SIZE", "P_CONTAINER", "P_RETAILPRICE", "P_COMMENT")
    val regionSchema = List("R_REGIONKEY", "R_NAME", "R_COMMENT")
    val supplierSchema = List("S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_NATIONKEY", "S_PHONE", "S_ACCTBAL", "S_COMMENT")
    val schemata = List(customerSchema, lineitemSchema, nationSchema, ordersSchema, partSchema, regionSchema, supplierSchema)

    // output
    var inclusionDependencies: RDD[String] = _

    /**
      * Load and parse input CSV data and convert it to RDDs.
      * @param spark Spark session used to access Spark SQL API
      */
    def loadInput(spark: SparkSession): Unit = {
        import spark.implicits._
        customerCSV = spark.read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("delimiter", ";")
            .load(s"$path/tpch_customer.csv")
            .as[(String, String, String, String, String, String, String, String)]
            .rdd
        lineitemCSV = spark.read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("delimiter", ";")
            .load(s"$path/tpch_lineitem.csv")
            .as[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]
            .rdd
        nationCSV = spark.read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("delimiter", ";")
            .load(s"$path/tpch_nation.csv")
            .as[(String, String, String, String)]
            .rdd
        ordersCSV = spark.read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("delimiter", ";")
            .load(s"$path/tpch_orders.csv")
            .as[(String, String, String, String, String, String, String, String, String)]
            .rdd
        partCSV = spark.read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("delimiter", ";")
            .load(s"$path/tpch_part.csv")
            .as[(String, String, String, String, String, String, String, String, String)]
            .rdd
        regionCSV = spark.read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("delimiter", ";")
            .load(s"$path/tpch_region.csv")
            .as[(String, String, String)]
            .rdd
        supplierCSV = spark.read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("delimiter", ";")
            .load(s"$path/tpch_supplier.csv")
            .as[(String, String, String, String, String, String, String)]
            .rdd
    }

    /**
      * Saves the computed Inclusion Dependencies.
      */
    def saveOutput(): Unit = {
        val writer = new PrintWriter(outputFile)
        inclusionDependencies
            .collect()
            .foreach(line => writer.write(line + "\n"))
        writer.close()
    }

    /**
      * Flattens table into each cell value and its column name.
      * @param tableRDD RDD containing rows of a table
      * @param schema list of row names of the table
      * @tparam T type of a row of the RDD
      * @return RDD of the cell values and their columns names as set
      */
    def flattenTable[T <: Product](tableRDD: RDD[T], schema: List[String]): RDD[(String, Set[String])] = {
        tableRDD.flatMap { data =>
            val cellValues = data.productIterator.toList.map(_.asInstanceOf[String])
            cellValues.zip(schema.map(Set(_)))
        }
    }

    /**
      * Creates Spark environment and finds Inclusion Dependencies via SINDY.
      */
    def run(): Unit = {
        // create Spark environment
        val spark = SparkSession
            .builder()
            .appName("Spark Exercise IND")
            .master(s"local[$cores]")
            .getOrCreate()

        // load input data
        loadInput(spark)

        // compute INDS via SINDY
        val inputTables = List(customerCSV, lineitemCSV, nationCSV, ordersCSV, partCSV, regionCSV, supplierCSV)
            .map(rdd => rdd.map(_.asInstanceOf[Product]))
        val tableCells = inputTables
            .zip(schemata)
            .map { case (rdd, schema) => flattenTable(rdd, schema) }
            .reduce(_ union _)
        val attributeSets = tableCells
            .distinct
            .reduceByKey(_ ++ _)
            .values
            .distinct
        val inclusionLists = attributeSets
            .flatMap { attributeSet =>
                attributeSet.map { attribute =>
                    (attribute, attributeSet - attribute)
                }
            }
        inclusionDependencies = inclusionLists
            .reduceByKey(_ intersect _)
            .filter(_._2.nonEmpty)
            .map { case (dependentColumn, referencedColumns) =>
                s"$dependentColumn < ${referencedColumns.mkString(", ")}"
            }.sortBy(identity)

        // save computed INDS
        saveOutput()
    }
}
