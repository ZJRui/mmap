package org.tools4j.mmap.mapper;

import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public interface RegionFactory<T extends Region> {
    RegionFactory<AsyncRegion> ASYNC1 = (size, fileChannelSupplier, fileSizeEnsurer, mapMode) -> new AsyncAtomicStateMachineRegion(fileChannelSupplier, fileSizeEnsurer, mapMode, size, 2, TimeUnit.SECONDS);
    RegionFactory<AsyncRegion> ASYNC2 = (size, fileChannelSupplier, fileSizeEnsurer, mapMode) -> new AsyncVolatileStateMachineRegion(fileChannelSupplier, fileSizeEnsurer, mapMode, size, 2, TimeUnit.SECONDS);
    RegionFactory<AsyncRegion> ASYNC3 = (size, fileChannelSupplier, fileSizeEnsurer, mapMode) -> new AsyncAtomicExchangeRegion(fileChannelSupplier, fileSizeEnsurer, mapMode, size, 2, TimeUnit.SECONDS);

    T create(int size,
             Supplier<FileChannel> fileChannelSupplier,
             FileSizeEnsurer fileSizeEnsurer,
             FileChannel.MapMode mapMode);

}
