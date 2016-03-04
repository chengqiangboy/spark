/*
   Created by CQ on 2015/11/24.
 */

package org.apache.spark.streaming.scheduler.dynamic

import org.apache.spark.Logging
import org.apache.spark.streaming.Duration

import scala.util.Random

/*
 * This class compute the next batch size according to the pre N jobSets information.
 */

private[streaming]
class JobSliceStrategy(jobSetHistory: JobSetHistory, maxDuration: Duration, miniBatch: Int)
  extends Logging{
  private  val maxBatchSize = maxDuration.milliseconds

  private def random(): Long = {
    var next = JobSliceStrategy.rd.nextInt()
    while( next <=0 )
      next = JobSliceStrategy.rd.nextInt()
    ((next % maxBatchSize)/miniBatch + 1) * miniBatch
  }

  /* The batch size computed by slowStart is may be larger than maxBatchSize */
  private def slowStart(): Long = {
    val size = jobSetHistory.head.size
    if (size>0 && jobSetHistory.head(size-1)._1 * 2 < maxBatchSize) {
      jobSetHistory.head(size-1)._1 * 2
    }
    else if (size == 0) {
      miniBatch.toLong
    }
    else {
      jobSetHistory.head(size-1)._1 + miniBatch
    }
  }

  private def fixedPoint(r: Float, p: Float): Long = {
    if (jobSetHistory.head.isEmpty) {
      maxBatchSize
    }
    else if (jobSetHistory.head.size > 1) {
      val size = jobSetHistory.head.size
      val first = jobSetHistory.head(size-2)
      val second = jobSetHistory.head(size-1)
      val xSmall = if (first._1 < second._1) first else second
      val xLarge = if (first._2 > second._1) first else second

      if ( xLarge._2 * xSmall._1 > xLarge._1 * xSmall._2 && second._2 > p*second._1) {
        (((1-r) * xSmall._1).toInt % maxBatchSize / miniBatch + 1 ) * miniBatch
      }
      else {
        ((second._1/p).toInt % maxBatchSize / miniBatch + 1 ) * miniBatch
      }
    }
   else {
      miniBatch * (maxBatchSize/3/miniBatch *2 + 1)
    }
  }

  def nextBatchSize(): Long = {
    fixedPoint(0.25F, 0.7F)
  }
}


private [streaming] object JobSliceStrategy {
  private val rd = new Random()
}