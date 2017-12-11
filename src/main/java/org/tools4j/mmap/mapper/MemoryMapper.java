package org.tools4j.mmap.mapper;

import org.agrona.DirectBuffer;

import java.io.Closeable;

public interface MemoryMapper extends Closeable {
    boolean wrap(long position, DirectBuffer buffer);
    int maxSize();

    @Override
    void close();
}
