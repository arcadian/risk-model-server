package fb.risk;

import java.io.IOException;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class FlatDBReader1 {
    public record Slice(int offset, int length) {}
    private final MappedByteBuffer mmap;
    private final Map<String, Slice> index;

    public FlatDBReader(Path path) throws IOException {
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
            mmap = ch.map(FileChannel.MapMode.READ_ONLY, 0, ch.size())
                       .order(ByteOrder.LITTLE_ENDIAN);

            byte[] magic = new byte[8];
            mmap.get(magic);
            if (!new String(magic, StandardCharsets.US_ASCII).equals("FLATDB02"))
                throw new IllegalStateException("Bad file header");

            int symbolCount = mmap.getInt();
            int indexSize = mmap.getInt();

            index = new HashMap<>(symbolCount);
            int pos = 16;
            for (int i = 0; i < symbolCount; i++) {
                mmap.position(pos);
                int nameLen = mmap.getInt();
                byte[] nameBytes = new byte[nameLen];
                mmap.get(nameBytes);
                String name = new String(nameBytes, StandardCharsets.UTF_8);
                int fbLen = mmap.getInt();
                int fbOffset = mmap.getInt();
                index.put(name, new Slice(fbOffset, fbLen));
                pos += 4 + nameLen + 4 + 4;
            }
        }
    }

    public Optional<ByteBuffer> sliceFor(String symbol) {
        Slice s = index.get(symbol);
        if (s == null) return Optional.empty();

        ByteBuffer dup = mmap.duplicate();
        dup.position(s.offset);
        dup.limit(s.offset + s.length);
        return Optional.of(dup.slice().order(ByteOrder.LITTLE_ENDIAN));
    }
}

