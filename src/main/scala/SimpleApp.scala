/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession
import scopt.{OParser, OParserBuilder}
import org.apache.spark.sql.functions._

/*

The input lines from kafka are of the form:
1606921800000,001e0610e532,lightsense,tsl250rd,intensity,21853,53.262,acceleration_z,651,ep,290,commit,913,pressure,138,pm1,799,uv_intensity,823,idletime,-372,count,-72,ir_intensity,185,concentration,-61,flags,-532,tx,694.36,ep_heatsink,-556.92,acceleration_x,-221.40,fw,910.53,sample_flow_rate,-959.60,uptime,-515.15,pm10,-768.03,powersupply,214.72,magnetic_field_y,-616.04,alphasense,606.73,AoT_Chicago,053,Racine Ave & 18th St Chicago IL,41.857959,-87.65642700000002,AoT Chicago (S) [C],2017/12/15 00:00:00,

 */

object SimpleApp {
  case class Config( memory: String = "16g",
                     cores: String = "10",
                     topic: String = "kafka_array_of_things",
                     brokers: String = "posh:9092,scary:9092,sporty:9092,ginger:9092,baby:9092"
                   )

  val builder: OParserBuilder[Config] = OParser.builder[Config]
  val parser1: OParser[Unit, Config] = {
    import builder._
    OParser.sequence(
      programName("example"),
      opt[String]('m', "memory")
        .action((x, c) => c.copy(memory = x))
        .text("memory per executor"),
      opt[String]('c', "cores")
        .action((x, c) => c.copy(cores = x))
        .text("max cores in total, should equal topic partitions"),
      opt[String]('t', "topic")
        .action((x, c) => c.copy(topic = x))
        .text("which kafka topic to read from"),
      opt[String]('b', "brokers")
        .action((x, c) => c.copy(brokers = x))
        .text("the kafka brokers")
    )
  }

  def main(args: Array[String]) {

    OParser.parse(parser1, args, Config()) match {
      case Some(config) =>
        run(config)
      case _ =>
        println("Invalid args")
    }
  }

  def run(config: Config): Unit = {

    val spark =
      SparkSession.builder.appName("Kafka Read Performance")
        .config("spark.executor.memory",config.memory)
        .config("spark.cores.max",config.cores)
        .getOrCreate()

    import spark.implicits._


    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", config.brokers)
      .option("subscribe", config.topic)
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .option("failOnDataLoss","false")
      .load()

    // input data row is csv epochms,data1,data2,data3
    // epochms is fixed length (e.g. 1515495304044)
    // don't care about processing the data itself, just need the epoch
    // as the kafka key is not useful

    val startTime = System.nanoTime()
    val result =
      df.selectExpr("left(CAST(value AS STRING), 13) as ts", "right(CAST(value AS STRING), 13) as therest")
      .as[(String,String)]
      .groupByKey(_._1)
      .count()
      .collect()

    val endTime = System.nanoTime()

    val totalGroups = result.length
    val totalRows = result.map(_._2).sum

    val elapsedSecs =  (endTime - startTime) / 1E9

    val rowRate = totalRows / elapsedSecs

    println(s"Took $elapsedSecs secs to do $totalRows rows for $totalGroups groups - row rate = $rowRate rows per sec")
    spark.stop()
  }
}