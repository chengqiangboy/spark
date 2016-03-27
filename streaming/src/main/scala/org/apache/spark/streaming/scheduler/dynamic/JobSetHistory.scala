/*
  Add By CQ.
 */

package org.apache.spark.streaming.scheduler.dynamic

import org.apache.spark.Logging
import org.apache.spark.streaming.scheduler.JobScheduler

import java.util.concurrent.CopyOnWriteArrayList


/**
  * This class collect the pre N jobSet's execution information.
  */

private [streaming]
class JobSetHistory(jobScheduler: JobScheduler, totalNum: Int) extends Logging {
  private var used = 0

  /* _1: batchSize, _2: processingDelay, _3: scheduleDelay */
  var head = new CopyOnWriteArrayList[(Long, Long, Long)]()

  def this(jobScheduler: JobScheduler) = {
    this(jobScheduler, 10)
  }

  def addJobHistory(tuple: (Long, Long, Long)): Unit = {
    /*
    if (head.isEmpty){
      head.add(tuple)
      used = 1
    }
    else if (used < totalNum){
      used = used + 1
      head.add(tuple)
    }
    else {
      head.remove(0)
      head.add(tuple)
    }
    */

    head.add(tuple)
  }
}