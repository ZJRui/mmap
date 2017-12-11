package org.tools4j.mmap.mapper;

public interface Region extends MemoryMapper {
    boolean map(final long position);
    boolean unmap();
}
