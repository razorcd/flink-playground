package org.example.other;


import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.Iterator;

// Compute the sum of the tips for each driver in hour-long windows.
public class Buffer extends KeyedProcessFunction<Long, String, String> {

    private static final ListStateDescriptor<String> BUFFER = new ListStateDescriptor<>("BUFFER", String.class);

//    private final long durationMsec = Duration.of(1, ChronoUnit.MINUTES).toMillis();
    private transient ListState<String> buffer;

    @Override
    // Called once during initialization.
    public void open(OpenContext ctx) {
        buffer = getRuntimeContext().getListState(BUFFER);
    }

    public Buffer() {
        super();
    }

    @Override
    public void processElement(String value, KeyedProcessFunction<Long, String, String>.Context ctx, Collector<String> out) throws Exception {
        System.out.println("Processing value: " + value + " for key: " + ctx.getCurrentKey());

        TimerService timerService = ctx.timerService();
        long nextTrigger = Instant.now().toEpochMilli() + 5000;
        if (!buffer.get().iterator().hasNext()) timerService.registerProcessingTimeTimer(nextTrigger);

        buffer.add(value);

        System.out.print("buffer: ");
        buffer.get().forEach(v -> System.out.print(v + ", "));
        System.out.println();


        long currentWatermark = timerService.currentWatermark();
        System.out.println(Instant.ofEpochMilli(currentWatermark).toString() + " - " + currentWatermark);
        long currentProcessingTime = timerService.currentProcessingTime();
        System.out.println(Instant.ofEpochMilli(currentProcessingTime).toString() + " - " + currentProcessingTime);
//        long triggerTime = currentProcessingTime + Duration.ofSeconds(3).toMillis();
//
//        timerService.registerEventTimeTimer(triggerTime);
    }

    @Override
    // Called when the current watermark indicates that a window is now complete.
    public void onTimer(long timestamp, KeyedProcessFunction<Long, String, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        System.out.println("OnTimer called at timestamp: " + Instant.ofEpochMilli(timestamp).toString() + ", for key: " + ctx.getCurrentKey());

//        TimerService timerService = ctx.timerService();
//        if (!buffer.get().iterator().hasNext()) timerService.registerProcessingTimeTimer(timestamp + 5000);


        String lastValInBuffer = null;
        Iterator<String> iterator = buffer.get().iterator();
        while (iterator.hasNext()) {
            lastValInBuffer = iterator.next();
        }

        buffer.clear();
        if (lastValInBuffer!=null) out.collect(lastValInBuffer);
//        sumOfTips = getRuntimeContext().getMapState(new MapStateDescriptor<>("sumOfTips", Long.class, String.class));
//        System.out.println("sumOfTips in onTimer: "+sumOfTips);
//        out.collect(Tuple3.apply(ctx.getCurrentKey(), timestamp, sumOfTips.get(timestamp)));
    }

}