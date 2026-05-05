package com.hmdp.limter;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.*;

/**
 * 本地限流器（不依赖Redis）
 * 使用滑动窗口算法实现
 */
@Slf4j
public class LocalRateLimiter {

    private static final Map<String, SlidingWindow> windows = new ConcurrentHashMap<>();

    /**
     * 滑动窗口数据结构
     */
    private static class SlidingWindow {
        private final long windowSizeMs;
        private final int limit;
        private final long[] timestamps;
        private int index = 0;

        public SlidingWindow(int limit, int windowSeconds) {
            this.limit = limit;
            this.windowSizeMs = windowSeconds * 1000L;
            this.timestamps = new long[limit];
        }

        public long getLastAccessTime() {
            long last = 0;
            for (int i = 0; i < limit; i++) {
                last = Math.max(last, timestamps[i]);
            }
            return last;
        }

        public long getWindowSizeMs() {
            return windowSizeMs;
        }

        public synchronized boolean tryAcquire() {
            long now = System.currentTimeMillis();
            long windowStart = now - windowSizeMs;

            // 统计窗口内有效请求数（清理过期数据）
            int validCount = 0;

            for (int i = 0; i < limit; i++) {
                long ts = timestamps[i];
                if (ts > 0) {
                    if (ts > windowStart) {
                        validCount++;
                    } else {
                        // 过期数据标记为0，后续会被覆盖
                        timestamps[i] = 0;
                    }
                }
            }

            if (validCount >= limit) {
                return false;
            }

            // 记录本次请求，覆盖最老的或已过期数据
            timestamps[index] = now;
            index = (index + 1) % limit;
            return true;
        }
    }

    private static final ScheduledExecutorService cleaner =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "local-rate-limiter-cleaner");
                t.setDaemon(true);
                return t;
            });

    static {
        cleaner.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            windows.entrySet().removeIf(entry -> {
                SlidingWindow window = entry.getValue();
                // 移除非活跃窗口：最近两个窗口周期都没有访问
                return now - window.getLastAccessTime() > window.getWindowSizeMs() * 2;
            });
        }, 1, 1, TimeUnit.MINUTES);
    }

    public boolean tryAcquire(String key, int limit, int windowSeconds) {
        SlidingWindow window = windows.computeIfAbsent(key,
                k -> new SlidingWindow(limit, windowSeconds));
        return window.tryAcquire();
    }
}
