package ch.epfl.lts2

/**
  * Created by volodymyrmiz on 05.10.16.
  */
package object Utils {
  /***
    * Hide Apache Spark console logs.
    * @param params List of logs to be suppressed.
    */
  def suppressLogs(params: List[String]): Unit = {
    import org.apache.log4j.{Level, Logger}
    params.foreach(Logger.getLogger(_).setLevel(Level.OFF))
  }
}
