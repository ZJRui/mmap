package org.tools4j.mmap.mapper;

import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.tools4j.mmap.io.MappedFile;

import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

public class AsyncAtomicExchangeRegion implements AsyncRegion {
    private static final long NULL = -1;

    private final Supplier<FileChannel> fileChannelSupplier;
    private final FileSizeEnsurer fileSizeEnsurer;
    private final FileChannel.MapMode mapMode;
    private final int length;
    private final long timoutNanos;


    private final AtomicLong requestPosition = new AtomicLong(NULL);
    private final AtomicLong responseAddress = new AtomicLong(NULL);

    private long readerPosition = NULL;
    private long readerAddress = NULL;

    private long writerPosition = NULL;
    private long writerAddress = NULL;

    public AsyncAtomicExchangeRegion(final Supplier<FileChannel> fileChannelSupplier,
                                     final FileSizeEnsurer fileSizeEnsurer,
                                     final FileChannel.MapMode mapMode,
                                     final int length,
                                     final long timeout,
                                     final TimeUnit timeUnits) {
        this.fileChannelSupplier = Objects.requireNonNull(fileChannelSupplier);
        this.fileSizeEnsurer = Objects.requireNonNull(fileSizeEnsurer);
        this.mapMode = Objects.requireNonNull(mapMode);
        this.length = length;
        this.timoutNanos = timeUnits.toNanos(timeout);
    }

    @Override
    public boolean wrap(final long position, final DirectBuffer source) {
        final int regionOffset = (int) (position % this.length);
        final long regionStartPosition = position - regionOffset;
        if (awaitMapped(regionStartPosition)) {
            source.wrap(readerAddress + regionOffset, this.length - regionOffset);
            return true;
        }
        return false;
    }

    @Override
    public boolean map(final long position) {
        if (readerPosition == position) return true;

        readerPosition = NULL;
        requestPosition.set(position); //can be lazy

        return false;
    }

    @Override
    public boolean unmap() {
        readerPosition = NULL;
        requestPosition.set(NULL); //can be lazy

        return false;
    }

    private boolean awaitMapped(final long position) {
        if (!map(position)) {
            final long timeOutTimeNanos = System.nanoTime() + timoutNanos;
            long respAddress;
            do {
                if (timeOutTimeNanos <= System.nanoTime()) return false; // timeout
                respAddress = responseAddress.get();
            } while (readerAddress == respAddress);

            readerAddress = respAddress;
            readerPosition = position;
        }
        return true;
    }

    @Override
    public boolean process() {
        final long reqPosition = requestPosition.get();
        if (writerPosition != reqPosition) {
            if (writerAddress != NULL) {
                IoUtil.unmap(fileChannelSupplier.get(), writerAddress, length);
                writerAddress = NULL;
            }
            if (reqPosition != NULL) {
                fileSizeEnsurer.ensureSize(reqPosition + length);
                writerAddress = IoUtil.map(fileChannelSupplier.get(), mapMode, reqPosition, length);
            }
            writerPosition = reqPosition;
            responseAddress.set(writerAddress); //can be lazy
            return true;
        }
        return false;
    }

    @Override
    public void close() {
        unmap();
    }

    @Override
    public int maxSize() {
        return length;
    }

}
