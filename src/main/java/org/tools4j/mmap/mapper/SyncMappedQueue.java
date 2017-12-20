package org.tools4j.mmap.mapper;

import org.tools4j.mmap.io.MappedFile;

import java.io.IOException;

public class SyncMappedQueue implements Queue {
    private final MemoryMappedFile appenderMemoryMappedFile;
    private final MemoryMappedFile enumeratorMemoryMappedFile;


    public SyncMappedQueue(final String fileName, final long regionSize) throws IOException {
        appenderMemoryMappedFile = new MemoryMappedFile(new MappedFile(fileName, MappedFile.Mode.READ_WRITE_CLEAR, regionSize, FileInitialiser::initFile), RegionRingFactory.forSync(RegionFactory.SYNC));
        enumeratorMemoryMappedFile = new MemoryMappedFile(new MappedFile(fileName, MappedFile.Mode.READ_ONLY, regionSize, FileInitialiser::initFile), RegionRingFactory.forSync(RegionFactory.SYNC));
    }

    @Override
    public Appender appender() {
        return new AlignedMappedAppender(appenderMemoryMappedFile);
    }

    @Override
    public Enumerator enumerator() {
        return new AlignedMappedEnumerator(enumeratorMemoryMappedFile);
    }

    @Override
    public void close() {
        appenderMemoryMappedFile.close();
        enumeratorMemoryMappedFile.close();
    }
}
