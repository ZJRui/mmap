package org.tools4j.mmap.mapper;

import org.agrona.DirectBuffer;
import org.tools4j.mmap.io.MappedFile;

import java.io.IOException;
import java.util.Objects;

public class MemoryMappedFile implements MemoryMapper {
    private static final int MB = 1024 * 1024;
    private static final int MAX_FILE_SIZE = 64 * 16 * MB;

    private static final int REGIONS_LENGHT = 4;
    private static final int MASK = REGIONS_LENGHT - 1;
    private final Region[] regions;
    private final int regionSize;

    private long currentAbsoluteIndex = -1;
    private final MappedFile file;

    public MemoryMappedFile(final MappedFile file, final RegionRingFactory regionRingFactory) throws IOException {
        this.file = Objects.requireNonNull(file);
        this.regionSize = (int) file.getRegionSize();


        regions = regionRingFactory.create(REGIONS_LENGHT, regionSize, file::getFileChannel, fileSizeEnsurer(file.getMode()), file.getMode().getMapMode());
    }

    @Override
    public boolean wrap(final long position, final DirectBuffer buffer) {
        final long absoluteIndex = position / regionSize;
        final long nextAbsoluteIndex = absoluteIndex + 1;
        final long prevAbsoluteIndex = absoluteIndex - 1;

        final boolean wrapped = regions[(int) (absoluteIndex % REGIONS_LENGHT)].wrap(position, buffer);
        if (wrapped) {
            if (currentAbsoluteIndex == prevAbsoluteIndex) {
                regions[(int) (nextAbsoluteIndex % REGIONS_LENGHT)].map(nextAbsoluteIndex * regionSize);
                if (prevAbsoluteIndex >= 0) {
                    regions[(int) (prevAbsoluteIndex % REGIONS_LENGHT)].unmap();
                }
            } else if (currentAbsoluteIndex == nextAbsoluteIndex) {
                if (prevAbsoluteIndex >= 0) {
                    regions[(int) (prevAbsoluteIndex % REGIONS_LENGHT)].map(prevAbsoluteIndex * regionSize);
                }
                regions[(int) (nextAbsoluteIndex % REGIONS_LENGHT)].unmap();
            }
            currentAbsoluteIndex = absoluteIndex;
        }

        return wrapped;
    }

    @Override
    public void close() {
        for (final Region region : regions) {
            region.close();
        }
        file.close();
    }

    @Override
    public int maxSize() {
        return regionSize;
    }

    private FileSizeEnsurer fileSizeEnsurer(final MappedFile.Mode mode) {
        if (mode != MappedFile.Mode.READ_ONLY) {
            return FileSizeEnsurer.forWritableFile(file::getFileLength, file::setFileLength, MAX_FILE_SIZE);
        } else {
            return FileSizeEnsurer.NO_OP;
        }
    }

}
