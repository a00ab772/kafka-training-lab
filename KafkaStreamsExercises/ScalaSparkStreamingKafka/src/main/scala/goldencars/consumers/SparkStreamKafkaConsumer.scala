package goldencars.consumers

import goldencars.functions
import goldencars.typesafeconfig.TypeSafeConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object SparkStreamKafkaConsumer {

  // Typesafe config
  val typesafeconf = TypeSafeConfig.Variables


  def main(args: Array[String]): Unit = {
    // Desactivamos logs de línea de comando
    // Logger.getLogger("org").setLevel(Level.OFF)
    // Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf()
      .setAppName(typesafeconf.applicationName)
      .setMaster(typesafeconf.sparkMaster)
      // When you're not using spark_master = "local[*]
      // then you may hit the
      //
      // WARN TaskSchedulerImpl: Initial job has not accepted any resources; check your cluster UI to ensure that workers are registered and have sufficient resources
      // that prevents running tasks
      /*
      * This message will pop up any time an application is requesting more resources from the cluster than the cluster can
      * currently provide.
      * What resources you might ask? Well Spark is only looking for two things: Cores and Ram.
      * Cores represents the number of open executor slots that your cluster provides for execution.
      * Ram refers to the amount of free Ram required on any worker running your application.
      * Note for both of these resources the maximum value is not your System's max,
      * it is the max as set by the your Spark configuration.
      * To see the current state of your cluster (and it’s free resources) check out the UI at spark-master-host:8080
      * Depending on your configuration this will be:
      *
      * 10.0.12.62:8080
      * or
      * localhost:8080
      *
      * https://spark.apache.org/docs/latest/configuration.html
      * */
      .set("spark.executor.memory", "1g")
      .set("spark.driver.memory", "1g")
      .set("spark.driver.cores", "1")
      .set("spark.cores.max","1")


    val streamingContext = new StreamingContext(conf, Seconds(5))

    streamingContext.checkpoint(typesafeconf.hdfsCheckpointPath)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> typesafeconf.kafkaBroker,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(typesafeconf.kafkaTopic)


    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val sarStream =
      stream.transform(rddSar =>
        rddSar.map(registro => {
          functions.transformaJsonSarATablaSar(registro.value().mkString(""))
        }))

    sarStream.foreachRDD{ rdd =>
      // Get the singleton instance of SparkSession
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      // Convert RDD[String] to DataFrame
      val xmlSARDataFrame = rdd.toDF()

      // Create a temporary view
      xmlSARDataFrame.createOrReplaceTempView("TABLASAR")

      // Do a SQL on DataFrame and print it
      val consultaXMLIn =
        spark.sql(
          """select *
            |from TABLASAR""".stripMargin)

      consultaXMLIn.cache()
      consultaXMLIn.show()

      // guardamos en /tmp
      try {
        println("Guardando en HDFS los datos procesados ...")
        consultaXMLIn.write.partitionBy("Code").mode(SaveMode.Append)
          .parquet("/tmp/SARPROCESADO")
      } catch {
        case ex: Exception => println("Error guardando en HDFS:\n"+ex.printStackTrace())
      }

      // guardamos en HDFS
      try {
        println("Guardando en HDFS los datos procesados ...")
        consultaXMLIn.write.partitionBy("Code").mode(SaveMode.Append)
          .parquet(typesafeconf.hdfsPath + "/SARPROCESADO")
      } catch {
        case ex: Exception => println("Error guardando en HDFS:\n"+ex.printStackTrace())
      }

    }

    stream.foreachRDD(rdd => {
          println("--- New RDD with " + rdd.partitions.size+ "======"
            + " partitions and " + rdd.count() + " records")
          rdd.foreach(record => println("Contenido del registro:\n "+ record.value().toString))
        })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
