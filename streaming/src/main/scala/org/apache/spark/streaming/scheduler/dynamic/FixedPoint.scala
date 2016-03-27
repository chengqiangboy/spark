/*
   Created by CQ on 2016/03/26.
 */

package org.apache.spark.streaming.scheduler.dynamic

import org.apache.spark.Logging

/**
  * FixedPoint algorithm.
  */

private[streaming] class FixedPoint(
      r: Double,
      p: Double,
      jobSetHistory: JobSetHistory,
      maxBatchSize: Long,
      miniBatch: Long) extends JobSliceStrategy with Logging {

  /* _1: batchSize, _2: processingDelay, _3: scheduleDelay */
  def nextBatchSize: Long = {
    if (jobSetHistory.head.isEmpty) {
      maxBatchSize
    }
    else if (jobSetHistory.head.size > 1) {
      val size = jobSetHistory.head.size
      val first = jobSetHistory.head.get(size-2)
      val second = jobSetHistory.head.get(size-1)
      val xSmall = if (first._1 < second._1) first else second
      val xLarge = if (first._1 > second._1) first else second
      var ret = 0L
      if ( xLarge._2 * xSmall._1 > xLarge._1 * xSmall._2 && second._2 > p*second._1) {
        ret = Math.ceil((1-r) * xSmall._1 / miniBatch).toLong * miniBatch
      }
      else {
        ret = Math.ceil(second._2/p/miniBatch).toLong * miniBatch
      }
      logError(s"first_x: ${first._1} firest_y: ${first._2} second_x: ${second._1} second_y: ${second._2} next: $ret")
      ret = Math.min(maxBatchSize, ret).max(miniBatch)
      ret
    }
    else {
      miniBatch * (maxBatchSize/3/miniBatch *2 + 1)
    }
  }

}
