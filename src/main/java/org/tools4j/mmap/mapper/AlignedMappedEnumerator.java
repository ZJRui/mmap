package org.tools4j.mmap.mapper;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.Objects;

public class AlignedMappedEnumerator implements Enumerator {
    private final static int LENGTH_SIZE = 4;
    private final MemoryMapper memoryMapper;
    private final UnsafeBuffer unsafeBuffer = new UnsafeBuffer();
    private final UnsafeBuffer messageBuffer = new UnsafeBuffer();

    private long position = 0;
    private int nextLength = 0;

    public AlignedMappedEnumerator(final MemoryMapper memoryMapper) {
        this.memoryMapper = Objects.requireNonNull(memoryMapper);
    }

    @Override
    public boolean hasNextMessage() {
        if (memoryMapper.wrap(position, unsafeBuffer)) {
            final int length = unsafeBuffer.getIntVolatile(0);
            if (length > 0) {
                nextLength = length;
                return true;
            }
        }
        return false;
    }

    @Override
    public DirectBuffer readNextMessage() {
        if (nextLength > 0) {
            messageBuffer.wrap(unsafeBuffer, LENGTH_SIZE, nextLength);
            position += LENGTH_SIZE + nextLength;
            nextLength = 0;
            return messageBuffer;
        } else {
            throw new IllegalStateException("Not ready to read message");
        }
    }

    @Override
    public Enumerator skipNextMessage() {
        if (nextLength > 0) {
            position += LENGTH_SIZE + nextLength;
            nextLength = 0;
        } else {
            throw new IllegalStateException("Not ready to read message");
        }
        return this;
    }

    @Override
    public void close() {
        memoryMapper.close();
    }
}
