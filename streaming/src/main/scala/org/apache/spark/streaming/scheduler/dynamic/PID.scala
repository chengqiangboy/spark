/*
   Created by CQ on 2016/03/26.
 */


package org.apache.spark.streaming.scheduler.dynamic

import org.apache.spark.Logging


/**
  * PID algorithm.
  */
private[streaming] class PID(
    p: Double,
    i: Double,
    d: Double,
    relax: Double,
    jobSetHistory: JobSetHistory,
    maxBatchSize: Long,
    miniBatch: Long) extends JobSliceStrategy with Logging {


  /* _1: batchSize, _2: processingDelay, _3: scheduleDelay */
  def nextBatchSize: Long = {
    if (jobSetHistory.head.isEmpty) {
      maxBatchSize
    }
    else {
      val size = jobSetHistory.head.size
      val lastBacth = jobSetHistory.head.get(size-1)
      val error = lastBacth._2 - lastBacth._1
      val historicalError = lastBacth._3
      var lastError = 0L
      if(size>1) {
        val secondLast = jobSetHistory.head.get(size-2)
        lastError = secondLast._2 - secondLast._1
      }
      val dError = (error - lastError)/lastBacth._1.toFloat
      val ans = lastBacth._1 + p*error + i*historicalError + d*dError
      val ret = miniBatch * Math.ceil(ans / miniBatch).toLong
      val nextbacth = Math.min(ret, maxBatchSize).max(miniBatch)
      logError(s"accodingto:${size-1} LastBathSize:${lastBacth._1}  LastProcess:${lastBacth._2}" +
        s"  Error:$error " + s"history:$historicalError" + s" dError:$dError " +
        s"next:$ans nextBatch:$nextbacth")
      nextbacth
    }
  }




}
