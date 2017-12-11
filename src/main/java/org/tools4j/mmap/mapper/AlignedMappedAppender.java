package org.tools4j.mmap.mapper;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.Objects;

public class AlignedMappedAppender implements Appender {
    private final static int LENGTH_SIZE = 4;
    private final static int CACHE_LINE_SIZE = 64;
    private final static int MASK = CACHE_LINE_SIZE - 1;
    private final MemoryMapper memoryMapper;
    private final UnsafeBuffer unsafeBuffer = new UnsafeBuffer();

    private long position = 0;

    public AlignedMappedAppender(final MemoryMapper memoryMapper) {
        this.memoryMapper = Objects.requireNonNull(memoryMapper);
    }

    @Override
    public boolean append(final DirectBuffer buffer, final int offset, final int length) {
        final int messageLength = length + LENGTH_SIZE;

        final int cacheLineRemainder = messageLength % CACHE_LINE_SIZE;
        final int paddedMessageLength;
        if (cacheLineRemainder != 0) {
            paddedMessageLength = messageLength + CACHE_LINE_SIZE - cacheLineRemainder;
        } else {
            paddedMessageLength = messageLength;
        }

        if (paddedMessageLength > memoryMapper.maxSize()) {
            throw new IllegalStateException("Length is too big");
        }

        if (memoryMapper.wrap(position, unsafeBuffer)) {
            final int capacity = unsafeBuffer.capacity();

            if (capacity < paddedMessageLength) {
                throw new IllegalStateException("Illegal Buffer capacity " + capacity);
            }

            buffer.getBytes(offset, unsafeBuffer, LENGTH_SIZE, length);

            unsafeBuffer.putIntOrdered(0, paddedMessageLength - LENGTH_SIZE);
            position += paddedMessageLength;
            return true;
        }
        return false;
    }

    @Override
    public void close() {
        memoryMapper.close();
    }
}
