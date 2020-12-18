/* SimpleApp.scala */
import org.apache.spark.sql.{SaveMode, SparkSession}
import scopt.{OParser, OParserBuilder}
import org.apache.spark.sql.functions._

/*

The input lines from kafka are of the form:
1606921800000,001e0610e532,lightsense,tsl250rd,intensity,21853,53.262,acceleration_z,651,ep,290,commit,913,pressure,138,pm1,799,uv_intensity,823,idletime,-372,count,-72,ir_intensity,185,concentration,-61,flags,-532,tx,694.36,ep_heatsink,-556.92,acceleration_x,-221.40,fw,910.53,sample_flow_rate,-959.60,uptime,-515.15,pm10,-768.03,powersupply,214.72,magnetic_field_y,-616.04,alphasense,606.73,AoT_Chicago,053,Racine Ave & 18th St Chicago IL,41.857959,-87.65642700000002,AoT Chicago (S) [C],2017/12/15 00:00:00,

 */

object SimpleApp {
  case class Config(memory: String = "16g",
                    cores: String = "10",
                    inTopic: String = "sample_in",
                    outTopic: String = "sample_out",
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
      opt[String]('i', "in")
        .action((x, c) => c.copy(inTopic = x))
        .text("which kafka topic to read from"),
      opt[String]('o', "out")
        .action((x, c) => c.copy(outTopic = x))
        .text("which kafka topic to write to"),
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
        .config("spark.sql.sources.useV1SourceList","avro,csv,json,orc,parquet,text") // by not including kafka, we use the v2 version of the source
        .getOrCreate()

    import spark.implicits._


    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", config.brokers)
      .option("subscribe", config.inTopic)
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .option("failOnDataLoss","false")
      .load()

    // input data row is csv epochms,data1,data2,data3
    // epochms is fixed length (e.g. 1515495304044)
    // don't care about processing the data itself, just need the epoch
    // as the kafka key is not useful

    val startTime = System.nanoTime()
//    val input =
//      df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
//        .as[(String, String)]


    df
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", config.brokers)
        .option("topic", config.outTopic)
        .mode(SaveMode.Append)
        .save()
    val endTime = System.nanoTime()

    val elapsedSecs =  (endTime - startTime) / 1E9

    // static input sample was used, fixed row count.

    println(s"Took $elapsedSecs secs")
    spark.stop()
  }
}