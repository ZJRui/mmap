package org.tools4j.mmap.mapper;

import org.agrona.collections.MutableLong;

import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

public interface FileSizeEnsurer {
    FileSizeEnsurer NO_OP = minSize -> {};

    void ensureSize(long minSize);


    static FileSizeEnsurer forWritableFile(final LongSupplier fileSizeGetter, final LongConsumer fileSizeSetter, final long maxSize) {
        final MutableLong fileSize = new MutableLong(0);

        return minSize -> {
            if (fileSize.get() < minSize) {
                final long len = fileSizeGetter.getAsLong();
                if (len < minSize) {
                    final long newLen = minSize;
                    if (newLen > maxSize) {
                        throw new IllegalStateException("Exceeded max file size " + maxSize + ", requested size " + newLen);
                    }
                    fileSizeSetter.accept(newLen);
                    fileSize.set(newLen);
                } else {
                    fileSize.set(len);
                }
            }
        };
    }
}
