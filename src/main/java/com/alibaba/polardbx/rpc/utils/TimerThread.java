package com.alibaba.polardbx.rpc.utils;

import com.alibaba.polardbx.rpc.XLog;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @version 1.0
 */
public class TimerThread {
    private static Thread thread;
    private static AtomicLong counter = new AtomicLong(0);

    static {
        thread = new Thread(() -> {
            while (true) {
                counter.set(System.nanoTime());
                try {
                    Thread.sleep(0, 100_000); // 0.1ms
                } catch (InterruptedException ignore) {
                } catch (Throwable t) {
                    XLog.XLogLogger.error(t);
                }
            }
        }, "XRPC timer thread.");
        thread.start();
    }

    public static long fastNanos() {
        return counter.get();
    }
}
