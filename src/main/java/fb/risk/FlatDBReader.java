package fb.risk;

import com.google.flatbuffers.FlatBufferBuilder;
import quotes.SymbolData;

import java.io.IOException;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

public class FlatDBReader {

    public record Entry(int offset, int length) {}

    private final MappedByteBuffer mmap;
    private final Map<String, Entry> index = new HashMap<>();

    public FlatDBReader(Path file) throws IOException {
        FileChannel ch = FileChannel.open(file, StandardOpenOption.READ);
        this.mmap = ch.map(FileChannel.MapMode.READ_ONLY, 0, ch.size()).order(ByteOrder.LITTLE_ENDIAN);

        byte[] magic = new byte[8];
        mmap.position(0);
        mmap.get(magic);
        if (!"FLATDB02".equals(new String(magic, StandardCharsets.US_ASCII)))
            throw new IllegalStateException("Invalid DB format");

        int count = mmap.getInt();
        int indexSize = mmap.getInt();

        int p = 16;
        for (int i = 0; i < count; i++) {
            int len = mmap.getInt(p);
            p += 4;
            byte[] sym = new byte[len];
            mmap.position(p);
            mmap.get(sym, 0, len);
            p += len;
            int dataLength = mmap.getInt(p);
            p += 4;
            int dataOffset = mmap.getInt(p);
            p += 4;

            index.put(new String(sym, StandardCharsets.UTF_8), new Entry(dataOffset, dataLength));
        }
    }

    public SymbolData get(String symbol) {
        Entry e = index.get(symbol);
        if (e == null) return null;

        ByteBuffer dup = mmap.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        dup.position(e.offset).limit(e.offset + e.length);
        return SymbolData.getRootAsSymbolData(dup);
    }
}

