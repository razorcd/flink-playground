package org.example.other;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

public class CustomTimerGeneratorSourceHeartbeat2 implements SourceFunction<Long> {

    private volatile boolean isRunning = true;

    private static final long INTERVAL_MS = TimeUnit.SECONDS.toMillis(10); // 10 seconds interval

    // The run method continuously generates data until canceled.
    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning) {
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(-1L);
                System.out.println("CustomTimerGeneratorSource emitted: " + -1 + " at " + Instant.ofEpochMilli(System.currentTimeMillis()).toString());
            }
            Thread.sleep(INTERVAL_MS);
        }
    }

    // The cancel method is called when the job is stopped.
    @Override
    public void cancel() {
        isRunning = false;
    }
}