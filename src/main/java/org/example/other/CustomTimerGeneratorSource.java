package org.example.other;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

public class CustomTimerGeneratorSource implements SourceFunction<Long> {

    private volatile boolean isRunning = true;
    private long counter = 1;

    private static final long INTERVAL_MS = TimeUnit.SECONDS.toMillis(10); // 10 seconds interval

    // The run method continuously generates data until canceled.
    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning) {
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(counter);
                System.out.println("Emitted: " + counter + " at " + Instant.ofEpochMilli(System.currentTimeMillis()).toString());
                counter++;
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