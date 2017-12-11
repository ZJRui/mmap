package org.tools4j.mmap.mapper;

import java.nio.channels.FileChannel;
import java.util.function.Consumer;
import java.util.function.Supplier;

public interface RegionRingFactory {
    Region[] create(int ringSize,
               int regionSize,
               Supplier<FileChannel> fileChannelSupplier,
               FileSizeEnsurer fileSizeEnsurer,
               FileChannel.MapMode mapMode);

    static <T extends AsyncRegion> RegionRingFactory forAsync(final RegionFactory<T> regionFactory, final Consumer<Processor> processorConsumer) {
        return (ringSize, regionSize, fileChannelSupplier, fileSizeEnsurer, mapMode) -> {
            final AsyncRegion[] regions = new AsyncRegion[ringSize];

            for (int i = 0; i < ringSize; i++) {
                regions[i] = regionFactory.create(regionSize, fileChannelSupplier, fileSizeEnsurer, mapMode);
            }

            processorConsumer.accept(() -> {
                boolean processed = false;
                for (final Processor region : regions) {
                    processed |= region.process();
                }
                return processed;
            });
            return regions;
        };
    }
}
