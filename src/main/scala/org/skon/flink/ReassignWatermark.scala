package org.skon.flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object ReassignWatermark {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(50L)
    env.setParallelism(4)

    val stream1 = env
      .addSource(new SourceFunction[(Long, Long)] {
        override def run(ctx: SourceFunction.SourceContext[(Long, Long)]): Unit = {
          (0 to 750000000).foreach(count => ctx.collectWithTimestamp((1L, count), count))
        }
        override def cancel(): Unit = {}
      })

    val stream2 = env
      .addSource(new SourceFunction[(Long, Long)] {
        override def run(ctx: SourceFunction.SourceContext[(Long, Long)]): Unit = {
          //(0 to 750000000).foreach(count => ctx.collectWithTimestamp((1L, count), count))
          while (true) {
            Thread.sleep(1000)
          }
        }
        override def cancel(): Unit = {}
      })

    val stream3 = env
      .addSource(new SourceFunction[(Long, Long)] {
        override def run(ctx: SourceFunction.SourceContext[(Long, Long)]): Unit = {
          //(0 to 750000000).foreach(count => ctx.collectWithTimestamp((1L, count), count))
          while (true) {
            Thread.sleep(1000)
          }
        }
        override def cancel(): Unit = {}
      })

    val stream = stream1
      .connect(stream2)
      .map(e1 => e1, e2 => e2)
      .connect(stream3)
      .map(e1 => e1, e2 => e2)
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long, Long)] {
        var currentTimestamp: Long = Long.MinValue
        override def getCurrentWatermark: Watermark = {
          Console.println(currentTimestamp)
          new Watermark(currentTimestamp)
        }
        override def extractTimestamp(element: (Long, Long), previousElementTimestamp: Long): Long = {
          currentTimestamp = element._2
          currentTimestamp
        }
      })
      .map(e => (e._1, e._2, e._2, e._2))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(10L)))
      .reduce((e1, e2) => (e1._1, e1._2, e2._2, e1._2 + e2._2))
      .print
      //.addSink(new CollectionSink[(Long, Long)])

    env.execute("Interval Join")
  }
}
