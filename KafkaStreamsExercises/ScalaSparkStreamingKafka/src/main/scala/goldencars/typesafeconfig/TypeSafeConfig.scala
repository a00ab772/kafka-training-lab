package goldencars.typesafeconfig

import com.typesafe.config.ConfigFactory

/**
  * Created by atarin
  */
object TypeSafeConfig {
  private val configFactory = ConfigFactory.load()

  object Variables {
    private val typeSafeVars = configFactory.getConfig("streamconfig")

    lazy val applicationName = typeSafeVars.getString("application_name")
    lazy val records = typeSafeVars.getInt("records")
    lazy val timeMultiplier = typeSafeVars.getInt("time_multiplier")
    lazy val pages = typeSafeVars.getInt("pages")
    lazy val visitors = typeSafeVars.getInt("visitors")
    lazy val filePath = typeSafeVars.getString("file_path")
    lazy val destPath = typeSafeVars.getString("dest_path")
    lazy val numberOfFiles = typeSafeVars.getInt("number_of_files")
    lazy val kafkaTopic = typeSafeVars.getString("kafka_topic")
    lazy val hdfsPath = typeSafeVars.getString("hdfs_path")
    lazy val hdfsCheckpointPath = typeSafeVars.getString("hdfs_checkpoint_path")
    lazy val localCheckpointPath = typeSafeVars.getString("local_checkpoint_path")
    lazy val kafkaBroker = typeSafeVars.getString("kafka_broker")
    lazy val localMaster = typeSafeVars.getString("local_master")
    lazy val sparkMaster = typeSafeVars.getString("spark_master")
  }
}
