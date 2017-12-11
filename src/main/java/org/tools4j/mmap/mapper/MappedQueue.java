package org.tools4j.mmap.mapper;

import org.tools4j.mmap.io.InitialBytes;
import org.tools4j.mmap.io.MappedFile;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class MappedQueue implements Queue {
    private final String fileName;
    private final long regionSize;
    private final MemoryMappedFile appenderMemoryMappedFile;
    private final MemoryMappedFile enumeratorMemoryMappedFile;
    private final Thread thread;


    public MappedQueue(final String fileName, final long regionSize) throws IOException {
        this.fileName = Objects.requireNonNull(fileName);
        this.regionSize = regionSize;
        final List<Processor> processors = new ArrayList<>(2);
        appenderMemoryMappedFile = new MemoryMappedFile(new MappedFile(fileName, MappedFile.Mode.READ_WRITE_CLEAR, regionSize, MappedQueue::initFile), RegionRingFactory.forAsync(RegionFactory.ASYNC2, processors::add));
        enumeratorMemoryMappedFile = new MemoryMappedFile(new MappedFile(fileName, MappedFile.Mode.READ_ONLY, regionSize, MappedQueue::initFile), RegionRingFactory.forAsync(RegionFactory.ASYNC2, processors::add));

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

    private static void initFile(final FileChannel fileChannel, final MappedFile.Mode mode) throws IOException {
        switch (mode) {
            case READ_ONLY:
                if (fileChannel.size() < 8) {
                    throw new IllegalArgumentException("Invalid io format");
                }
                break;
            case READ_WRITE:
                if (fileChannel.size() >= 8) break;
            case READ_WRITE_CLEAR:
                final FileLock lock = fileChannel.lock();
                try {
                    fileChannel.truncate(0);
                    fileChannel.transferFrom(InitialBytes.MINUS_ONE, 0, 8);
                    fileChannel.force(true);
                } finally {
                    lock.release();
                }
                break;
            default:
                throw new IllegalArgumentException("Invalid mode: " + mode);
        }
    }

}
