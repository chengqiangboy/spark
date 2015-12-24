/*
  Add By CQ.
 */

package org.apache.spark.streaming.scheduler.dynamic

import org.apache.spark.Logging
import org.apache.spark.streaming.scheduler.JobScheduler

import scala.collection.mutable


/**
  * This class collect the pre N jobSet's execution information.
  */

private [streaming]
class JobSetHistory(jobScheduler: JobScheduler, totalNum: Int) extends Logging {
  private var used = 0
  val head = new mutable.ListBuffer[(Long, Long)]()

  def this(jobScheduler: JobScheduler) = {
    this(jobScheduler, 10)
  }

  def addJobHistory(tuple: (Long, Long)): Unit = {
    if (head.isEmpty){
      head += tuple
      used = 1
    }
    else if (used < totalNum){
      used = used + 1
      head += tuple
    }
    else {
      head.trimStart(1)
      head += tuple
    }

  }
}