import fb.risk.SymbolData;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class IndexedSymbolStore {

    private final MappedByteBuffer mmap;
    private final Map<String, IndexEntry> index = new HashMap<>();

    public IndexedSymbolStore(String fileName) throws Exception {
        try (RandomAccessFile raf = new RandomAccessFile(fileName, "r")) {
            FileChannel ch = raf.getChannel();
            this.mmap = ch.map(FileChannel.MapMode.READ_ONLY, 0, ch.size());

            mmap.position(0);
            int numSymbols = mmap.getInt();

            for (int i = 0; i < numSymbols; i++) {
                short nameLen = mmap.getShort();
                byte[] nameBytes = new byte[nameLen];
                mmap.get(nameBytes);
                String name = new String(nameBytes, StandardCharsets.UTF_8);

                long offset = mmap.getLong();
                int length = mmap.getInt();

                index.put(name, new IndexEntry(offset, length));
            }
        }
    }

    public ByteBuffer get(String symbol) {
        IndexEntry e = index.get(symbol);
        if (e == null) return null;

        ByteBuffer slice = mmap.duplicate();
        slice.position((int) (e.offset + 4));           // skip length prefix
        slice.limit((int) (e.offset + 4 + e.length));   // exact FlatBuffer bytes
        slice = slice.slice().order(ByteOrder.LITTLE_ENDIAN);
        slice.rewind();
        return slice;
    }

    public static class IndexEntry {
        long offset;
        int length;
        IndexEntry(long offset, int length) { this.offset = offset; this.length = length; }
    }
}

