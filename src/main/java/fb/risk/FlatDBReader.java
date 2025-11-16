package fb.risk;

import java.io.IOException;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

public class FlatDBReader {

    public static record Entry(String symbol, int offset, int length) {}

    private final FileChannel channel;
    private final MappedByteBuffer mmap;
    private final Map<String, Entry> index = new HashMap<>();

    public FlatDBReader(Path file) throws IOException {
        channel = FileChannel.open(file, StandardOpenOption.READ);
        mmap = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        mmap.order(ByteOrder.LITTLE_ENDIAN);
        loadIndex();
    }

    private void loadIndex() {
        byte[] magic = new byte[8];
        mmap.get(magic);
        int count = mmap.getInt();
        int indexSize = mmap.getInt();

        int pos = 16;
        for (int i = 0; i < count; i++) {
            int len = mmap.getInt(pos);
            pos += 4;
            byte[] symBytes = new byte[len];
            mmap.position(pos);
            mmap.get(symBytes, 0, len);
            pos += len;

            String sym = new String(symBytes, StandardCharsets.UTF_8);
            int bufLen = mmap.getInt(pos); pos += 4;
            int off = mmap.getInt(pos); pos += 4;
            index.put(sym, new Entry(sym, off, bufLen));
        }
    }

    public Optional<ByteBuffer> sliceFor(String symbol) {
        Entry e = index.get(symbol);
        if (e == null) return Optional.empty();
        ByteBuffer dup = mmap.duplicate();
        dup.position(e.offset);
        dup.limit(e.offset + e.length);
        return Optional.of(dup.slice());
    }
}

