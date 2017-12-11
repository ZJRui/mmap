package org.tools4j.mmap.mapper;

import org.HdrHistogram.Histogram;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tools4j.mmap.io.RegionMapper;
import org.tools4j.mmap.util.FileUtil;
import org.tools4j.mmap.util.HistogramPrinter;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class MappedQueueTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MappedQueueTest.class);

    public static void main(String... args) throws Exception {
        final String fileName = FileUtil.sharedMemDir("regiontest").getAbsolutePath();
        LOGGER.info("File: {}", fileName);
        final long regionSize = Math.max(RegionMapper.REGION_SIZE_GRANULARITY, 1L << 16) * 64;//64 KB
        LOGGER.info("regionSize: {}", regionSize);
        final MappedQueue mappedQueue = new MappedQueue(fileName, regionSize);
        final Appender appender = mappedQueue.appender();

        final String testMessage = "#------------------------------------------------#\n";

        final ByteBuffer byteBuffer = ByteBuffer.allocate(8 + testMessage.getBytes().length);
        final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);
        unsafeBuffer.putBytes(8, testMessage.getBytes());

        final int size = 8 + testMessage.getBytes().length;

//        final Histogram histogram = new Histogram(1, TimeUnit.MINUTES.toNanos(10), 3);

        final long messagesPerSecond = 100000;
        final long maxNanosPerMessage = 1000000000 / messagesPerSecond;

        final Enumerator enumerator = mappedQueue.enumerator();

        final Thread pollerThread = new Thread(() -> {
            final Histogram histogram = new Histogram(1, TimeUnit.SECONDS.toNanos(1), 3);
            long lastTimeNanos = 0;
            //final IdleStrategy idleStrategy = new BackoffIdleStrategy(400, 100, 1, TimeUnit.MICROSECONDS.toNanos(100));
            int i = 0;
            while (i < 1000000 - 10) {
                if (enumerator.hasNextMessage()) {
                    i++;
                    final DirectBuffer messageBuffer = enumerator.readNextMessage();
                    if (i > 99750 && i < 399780) {
                        final long startNanos = messageBuffer.getLong(0);
                        long end = System.nanoTime();
                        final long timeNanos = end - startNanos;
                        histogram.recordValue(timeNanos);
                        lastTimeNanos = timeNanos;
                    }

                }
            }
            HistogramPrinter.printHistogram(histogram);
            System.out.println("lastTimeNanos " + lastTimeNanos);
        });
        pollerThread.setName("async-processor");
        pollerThread.setDaemon(true);
        pollerThread.start();


        for (int i = 0; i < 1000000; i++) {
            final long start = System.nanoTime();
            unsafeBuffer.putLong(0, start);
            appender.append(unsafeBuffer, 0, size);
            long end = System.nanoTime();
            final long waitUntil = start + maxNanosPerMessage;
            while (end < waitUntil) {
                end = System.nanoTime();
            }
        }

        pollerThread.join();


//        while (enumerator.hasNextMessage()) {
//            final DirectBuffer messageBuffer = enumerator.readNextMessage();
//            final long timeNanos = messageBuffer.getLong(0);
//        }


//        HistogramPrinter.printHistogram(histogram);
    }
}