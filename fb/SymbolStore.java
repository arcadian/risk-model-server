import fb.risk.SymbolData;
import com.github.benmanes.caffeine.cache.*;

import java.io.Closeable;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SymbolStore implements Closeable {

    private final MappedByteBuffer mmap;
    private final Map<String, Long> offsets = new HashMap<>();
    private final LoadingCache<String, ByteBuffer> cache;

    public SymbolStore(String fileName) throws Exception {
        try (RandomAccessFile raf = new RandomAccessFile(fileName, "r")) {
            FileChannel ch = raf.getChannel();
            this.mmap = ch.map(FileChannel.MapMode.READ_ONLY, 0, ch.size());

            long pos = 0;
            while (pos < mmap.capacity()) {
                mmap.position((int) pos);
                int len = mmap.getInt();

                ByteBuffer slice = mmap.duplicate();
                slice.position((int) (pos + 4)).limit((int) (pos + 4 + len));
                slice = slice.slice().order(ByteOrder.LITTLE_ENDIAN);

                // parse symbol name to build offset map
                String sym = SymbolData.getRootAsSymbolData(slice).name();
                offsets.put(sym, pos);

                pos += 4 + len;
            }
        }

        // build Caffeine cache
        this.cache = Caffeine.newBuilder()
                .maximumSize(10_000)
                .expireAfterAccess(30, TimeUnit.MINUTES)
                .build(this::loadSlice);
    }

    private ByteBuffer loadSlice(String symbol) {
        Long pos = offsets.get(symbol);
        if (pos == null) return null;

        mmap.position(pos.intValue());
        int len = mmap.getInt();

        ByteBuffer slice = mmap.duplicate();
        slice.position((int) (pos + 4)).limit((int) (pos + 4 + len));
        return slice.slice().order(ByteOrder.LITTLE_ENDIAN);
    }

    public ByteBuffer get(String symbol) {
        ByteBuffer slice = cache.get(symbol);
        if (slice != null) slice.rewind(); // ensure position=0
        return slice;
    }

    @Override
    public void close() {}
}

