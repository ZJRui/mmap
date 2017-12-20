package org.tools4j.mmap.mapper;

import org.agrona.DirectBuffer;
import org.agrona.IoUtil;

import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class SimpleRegion implements Region {
    private static final long NULL = -1;

    private final Supplier<FileChannel> fileChannelSupplier;
    private final FileSizeEnsurer fileSizeEnsurer;
    private final FileChannel.MapMode mapMode;
    private final int length;

    private long currentPosition = NULL;
    private long currentAddress = NULL;

    public SimpleRegion(final Supplier<FileChannel> fileChannelSupplier,
                        final FileSizeEnsurer fileSizeEnsurer,
                        final FileChannel.MapMode mapMode,
                        final int length) {
        this.fileChannelSupplier = Objects.requireNonNull(fileChannelSupplier);
        this.fileSizeEnsurer = Objects.requireNonNull(fileSizeEnsurer);
        this.mapMode = Objects.requireNonNull(mapMode);
        this.length = length;
    }

    @Override
    public boolean wrap(final long position, final DirectBuffer source) {
        final int regionOffset = (int) (position % this.length);
        final long regionStartPosition = position - regionOffset;
        if (map(regionStartPosition)) {
            source.wrap(currentAddress + regionOffset, this.length - regionOffset);
            return true;
        }
        return false;
    }

    @Override
    public boolean map(final long position) {
        if (currentPosition == position) return true;

        if (currentAddress != NULL) {
            IoUtil.unmap(fileChannelSupplier.get(), currentAddress, length);
            currentAddress = NULL;
        }
        if (position != NULL) {
            fileSizeEnsurer.ensureSize(position + length);
            currentAddress = IoUtil.map(fileChannelSupplier.get(), mapMode, position, length);
        }
        currentPosition = position;
        return true;
    }

    @Override
    public boolean unmap() {
        if (currentAddress != NULL) {
            IoUtil.unmap(fileChannelSupplier.get(), currentAddress, length);
            currentAddress = NULL;
            currentPosition = NULL;
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
