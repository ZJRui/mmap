package org.tools4j.mmap.mapper;

import org.tools4j.mmap.io.MappedFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AsyncMappedQueue implements Queue {
    private final MemoryMappedFile appenderMemoryMappedFile;
    private final MemoryMappedFile enumeratorMemoryMappedFile;
    private final Thread thread;


    public AsyncMappedQueue(final String fileName, final long regionSize) throws IOException {
        final List<Processor> processors = new ArrayList<>(2);
        appenderMemoryMappedFile = new MemoryMappedFile(new MappedFile(fileName, MappedFile.Mode.READ_WRITE_CLEAR, regionSize, FileInitialiser::initFile), RegionRingFactory.forAsync(RegionFactory.ASYNC2, processors::add));
        enumeratorMemoryMappedFile = new MemoryMappedFile(new MappedFile(fileName, MappedFile.Mode.READ_ONLY, regionSize, FileInitialiser::initFile), RegionRingFactory.forAsync(RegionFactory.ASYNC2, processors::add));

        thread = new Thread(() -> {
            final Processor[] processors1 = processors.toArray(new Processor[processors.size()]);
            while (true) {
                for(final Processor processor : processors1) {
                    processor.process();
                }
            }
        });
        thread.setName("async-processor");
        thread.setDaemon(true);
        thread.start();
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
        thread.interrupt();
        appenderMemoryMappedFile.close();
        enumeratorMemoryMappedFile.close();
    }
}
