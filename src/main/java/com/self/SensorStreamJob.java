package com.self;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * @author shaojieyue
 * Created at 2020-10-09 15:18
 */

public class SensorStreamJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
//        final SingleOutputStreamOperator<SensorEvent> streamOperator = environment.socketTextStream("localhost", 9000)
//                .map(new SensorEventParseFunction())
//                .filter(new SensorEventFilterFunction());
        final SingleOutputStreamOperator<SensorEvent> streamOperator = environment.addSource(new SensorSource())
                .filter(new SensorEventFilterFunction())
        ;
        //设置发送发送水位线的间隔
        environment.getConfig().setAutoWatermarkInterval(1000L);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        final DataStream<SensorEvent> outputStreamOperator = streamOperator
                .setParallelism(1)
                //最多允许延迟一分钟的水位线
                .assignTimestampsAndWatermarks(new SensorEventTimeAssigner(Time.seconds(10)))
                .keyBy("id")
                .window(new MaxTimestampWindowAssigner(Time.seconds(10)))
                .trigger(new SensorEventWindowTrigger())
                .process(new AvgProcessWindowFunction())
                ;
        outputStreamOperator.print();
        environment.execute("SensorStreamJob");
    }

    static class SensorEventTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor<SensorEvent> {

        public SensorEventTimeAssigner(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        /**
         * Extracts the timestamp from the given element.
         *
         * @param element The element that the timestamp is extracted from.
         * @return The new timestamp.
         */
        @Override
        public long extractTimestamp(SensorEvent element) {
            return element.timestamp;
        }
    }

    static class AvgProcessWindowFunction extends ProcessWindowFunction<SensorEvent, SensorEvent, Tuple, TimeWindow> {
        @Override
        public void process(Tuple tuple, Context context, Iterable<SensorEvent> elements, Collector<SensorEvent> out) throws Exception {
            double sum = 0;
            int count = 0;
            for (SensorEvent element : elements) {
                sum = element.temperature + sum;
                count ++;
            }
            String id = tuple.getField(0);
            final SensorEvent sensorEvent = new SensorEvent(id, context.window().getEnd(), sum / count);

            out.collect(sensorEvent);
        }
    }


    static class SensorEventWindowTrigger extends Trigger<SensorEvent, TimeWindow> {
        private static final Logger log = LoggerFactory.getLogger(SensorEventWindowTrigger.class);
        /**
         * 这里来实现当一个新元素进来时触发窗口计算逻辑
         * @param element
         * @param timestamp
         * @param window
         * @param ctx
         * @return
         * @throws Exception
         */
        @Override
        public TriggerResult onElement(SensorEvent element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            //maxTimestamp=End-1 表示窗口的最大有效时间
            //getCurrentWatermark为当前数据流已经进行到的水位线,即数据处理到哪个阶段了
            if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
                return TriggerResult.FIRE;
            } else {
                //这里需要注册一个一个触发时间,保证TimeWindow能够及时的触发操作
                //window.getEnd会导致窗口计算时间延迟
                //因为窗口的有效期为maxTimestamp=End-1
                /**
                 * 注意这里注册的时间一定要<=WindowOperator#cleanupTime(Window)
                 * 因为WindowOperator在方法里面会注册一个clean time,专门用来清除窗口状态
                 * 如果在cleanupTime之后注册,则窗口的数据已经被清除,导致无法进行计算
                 * 所以这里如果用时间戳window.getEnd(),则无法进行process计算
                 */
                ctx.registerEventTimeTimer(window.maxTimestamp());
                if (element.temperature > 100) {
                    //这里可以实现自定义触发逻辑
                }
                return TriggerResult.CONTINUE;
            }
        }

        
        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            //我们的窗口是给予事件时间的，所以遇到ProcessingTime时
            //触发窗口事件
            return TriggerResult.CONTINUE;
        }


        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            //只有当trigger时间为窗口的End时间是才触发计算
            if (time == window.maxTimestamp()) {
                System.out.println(this+"TriggerResult.FIRE TimeWindow="+window);
                return TriggerResult.FIRE;
            } else {
                System.out.println(this+"TriggerResult.CONTINUE");
                return TriggerResult.CONTINUE;
            }
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            System.out.println(this+"clear TimeWindow="+window);
        }
    }

    static class MaxTimestampWindowAssigner extends WindowAssigner<SensorEvent,TimeWindow> {
        private static final Logger log = LoggerFactory.getLogger(MaxTimestampWindowAssigner.class);
        private long windowSize;

        public MaxTimestampWindowAssigner(Time windowSize) {
            this.windowSize = windowSize.toMilliseconds();
        }

        /**
         *
         * @param o
         * @param ts 该事件当前的时间戳
         * @param windowAssignerContext
         * @return
         */
        @Override
        public Collection assignWindows(SensorEvent o, long ts, WindowAssignerContext windowAssignerContext) {
            //ts - (ts%windowSize)可以把事件固定的空间
            //具体参考TimeWindow.getWindowStartWithOffset
            long start = ts - (ts%windowSize);
            long end = start + windowSize;
            return Collections.singleton(new TimeWindow(start,end));
        }

        @Override
        public Trigger getDefaultTrigger(StreamExecutionEnvironment streamExecutionEnvironment) {
            return EventTimeTrigger.create();
        }

        @Override
        public TypeSerializer getWindowSerializer(ExecutionConfig executionConfig) {
            return new TimeWindow.Serializer();
        }

        @Override
        public boolean isEventTime() {
            return true;
        }
    }



    /**
     * 这里来过滤掉感温器的无效数据
     */
    static class SensorEventFilterFunction implements FilterFunction<SensorEvent> {
        private static final Logger log = LoggerFactory.getLogger(SensorEventFilterFunction.class);
        @Override
        public boolean filter(SensorEvent sensorEvent) throws Exception {
            //过滤无效记录，这只要非空的 并且温度>0
            final boolean valid = Objects.nonNull(sensorEvent) && sensorEvent.temperature > 0;
            if (!valid) {
                log.info("invalid event,discard, event={}",sensorEvent);
            }
            return valid;
        }
    }

    /**
     * 解析感温器事件，结构为
     * sensorId,temperature[,timeOffset]
     */
    static class SensorEventParseFunction implements MapFunction<String, SensorEvent> {
        private static final Logger log = LoggerFactory.getLogger(SensorEventParseFunction.class);
        @Override
        public SensorEvent map(String str) throws Exception {
            try {
                if (str == null) {
                    return null;
                }
                final String[] strings = str.split(",");
                if (strings.length != 2 && strings.length != 3) {
                    return null;
                }
                SensorEvent sensorEvent = new SensorEvent();
                sensorEvent.id = strings[0];
                sensorEvent.temperature = Double.valueOf(strings[1]);
                if (strings.length == 3) {
                    sensorEvent.timestamp = System.currentTimeMillis() - Time.seconds(Integer.valueOf(strings[2])).toMilliseconds();
                } else {
                    sensorEvent.timestamp = System.currentTimeMillis();
                }
                return sensorEvent;
            }catch (Exception e){
                log.error("parse fail,invalid data="+str);
            }
            return null;
        }
    }

}
