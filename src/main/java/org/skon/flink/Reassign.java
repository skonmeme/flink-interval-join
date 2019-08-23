package org.skon.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Reassign {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(50L);
        env.setParallelism(4);

        DataStreamSource<Tuple2<Long, Long>> stream1 = env
                .addSource(new SourceFunction<Tuple2<Long, Long>>() {
                    @Override
                    public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {
                        for (int i = 0; i < 750000000; i++) {
                            ctx.collectWithTimestamp(Tuple2.of(1L, (long) i), i);
                        }
                    }

                    @Override
                    public void cancel() {}
                });

        DataStreamSource<Tuple2<Long, Long>> stream2 = env
                .addSource(new SourceFunction<Tuple2<Long, Long>>() {
                    @Override
                    public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {
                        //for (int i = 0; i < 750000000; i++) {
                        //    ctx.collectWithTimestamp(Tuple2.of(2L, (long) i), i);
                        //}
                        while (true) {
                            Thread.sleep(1000L);
                        }
                    }

                    @Override
                    public void cancel() {}
                });

        DataStreamSource<Tuple2<Long, Long>> stream3 = env
                .addSource(new SourceFunction<Tuple2<Long, Long>>() {
                    @Override
                    public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {
                        for (int i = 0; i < 750000000; i++) {
                            ctx.collectWithTimestamp(Tuple2.of(3L, (long) i), i);
                        }
                        //while (true) {
                        //    Thread.sleep(1000L);
                        //}
                    }

                    @Override
                    public void cancel() {}
                });

        DataStream<Tuple4<Long, Long, Long, Long>> stream = stream1
                .connect(stream2)
                .map(new CoMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> map1(Tuple2<Long, Long> value) throws Exception {
                        return value;
                    }

                    @Override
                    public Tuple2<Long, Long> map2(Tuple2<Long, Long> value) throws Exception {
                        return value;
                    }
                })
                .connect(stream3)
                .map(new CoMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> map1(Tuple2<Long, Long> value) throws Exception {
                        return value;
                    }

                    @Override
                    public Tuple2<Long, Long> map2(Tuple2<Long, Long> value) throws Exception {
                        return value;
                    }
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<Long, Long>>() {
                    Long currentTimestamp = Long.MIN_VALUE;

                    @Override
                    public Watermark getCurrentWatermark() {
                        System.out.println(currentTimestamp);
                        return new Watermark(currentTimestamp);
                    }

                    @Override
                    public long extractTimestamp(Tuple2<Long, Long> element, long previousElementTimestamp) {
                        currentTimestamp = element.f1;
                        return currentTimestamp;
                    }
                })
                .map(new MapFunction<Tuple2<Long, Long>, Tuple4<Long, Long, Long, Long>>() {
                    @Override
                    public Tuple4<Long, Long, Long, Long> map(Tuple2<Long, Long> value) throws Exception {
                        return Tuple4.of(value.f0, value.f1, value.f1, value.f1);
                    }
                })
                .keyBy(e -> e.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce((e1, e2) -> Tuple4.of(e1.f0, e1.f1, e2.f1, e1.f1 + e2.f1));

        stream.print();

        env.execute("Reassign watermark");
    }
}
