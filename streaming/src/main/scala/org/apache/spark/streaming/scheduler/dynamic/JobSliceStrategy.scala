/*
   Created by CQ on 2015/11/24.
 */

package org.apache.spark.streaming.scheduler.dynamic

import org.apache.spark.SparkConf


/*
 * This class compute the next batch size according to the pre N jobSets information.
 */

private[streaming] trait JobSliceStrategy extends Serializable {

  def nextBatchSize(): Long

}



private [streaming] object JobSliceStrategy {

  def creat(
      conf: SparkConf,
      jobSetHistpry: JobSetHistory,
      maxBatchSize: Long,
      minBatch: Long): JobSliceStrategy = {
    conf.get("spark.streaming.elasticwindow", "pid") match {
      case "fixedpoint" =>
        val r = conf.getDouble("spark.streaming.elasticwindow.fixedpoint.r", 0.25)
        val p = conf.getDouble("spark.streaming.elasticwindow.fixedpoint.p", 0.7)
        new FixedPoint(r, p, jobSetHistpry, maxBatchSize, minBatch)
      case "pid" =>
        val p = conf.getDouble("spark.streaming.elasticwindow.pid.p", 1)
        val i = conf.getDouble("spark.streaming.elasticwindow.pid.i", 0.2)
        val d = conf.getDouble("spark.streaming.elasticwindow.pid.d", 0)
        val relax = conf.getDouble("spark.streaming.elasticwindow.pid.r", 0.7)
        new PID(p, i, d, relax, jobSetHistpry, maxBatchSize, minBatch)

      case strategy =>
        throw new IllegalArgumentException(s"Unkown elastic-window strategy: $strategy")
    }
  }


}
