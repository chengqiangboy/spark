/*
   Created by CQ on 2015/11/24.
 */

package org.apache.spark.streaming.scheduler.dynamic

import org.apache.spark.Logging

import scala.util.Random

/*
 * This class compute the next batch size according to the pre N jobSets information.
 */

private[streaming]
class JobSliceStrategy(jobSetHistory: JobSetHistory) extends Logging{
  private def random(): Long = {
    var next = JobSliceStrategy.rd.nextInt()
    while( next <=0 )
      next = JobSliceStrategy.rd.nextInt()
    ((next % 1000)/200 + 1)*200
  }

  def nextBatchSize(): Long = {
    random()
  }
}


private [streaming] object JobSliceStrategy {
  private val rd = new Random()
}