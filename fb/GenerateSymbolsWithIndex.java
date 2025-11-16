import fb.risk.SymbolData;
import com.google.flatbuffers.FlatBufferBuilder;

import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class GenerateSymbolsWithIndex {

    static final int NUM_SYMBOLS = 100;
    static final int NUM_FACTORS = 100;
    static final String FILE_NAME = "symbols_indexed.bin";

    public static void main(String[] args) throws Exception {
        Map<String, IndexEntry> indexMap = new LinkedHashMap<>();

        try (FileOutputStream fos = new FileOutputStream(FILE_NAME);
             FileChannel channel = fos.getChannel()) {

            // Reserve space for index (we'll write later)
            channel.position(0);

            List<ByteBuffer> symbolBuffers = new ArrayList<>();
            long dataSectionStart = 0; // track offset for index

            // Build FlatBuffers
            for (int i = 0; i < NUM_SYMBOLS; i++) {
                String symbol = "sym" + i;
                FlatBufferBuilder builder = new FlatBufferBuilder(1024);

                double[] factors = new double[NUM_FACTORS];
                for (int f = 0; f < NUM_FACTORS; f++) factors[f] = Math.random() * 10.0;

                int nameOffset = builder.createString(symbol);
                int factorsOffset = SymbolData.createFactorsVector(builder, factors);

                SymbolData.startSymbolData(builder);
                SymbolData.addName(builder, nameOffset);
                SymbolData.addFactors(builder, factorsOffset);
                int root = SymbolData.endSymbolData(builder);
                builder.finish(root);

                ByteBuffer bb = builder.dataBuffer();
                symbolBuffers.add(bb);
            }

            // Compute offsets
            dataSectionStart = 4; // 4 bytes for N
            for (ByteBuffer bb : symbolBuffers) {
                dataSectionStart += 2 + bb.capacity() + 8 + 4 + 0; // approximate index size
            }

            // Write index later
            long dataOffset = dataSectionStart;

            for (int i = 0; i < NUM_SYMBOLS; i++) {
                String symbol = "sym" + i;
                ByteBuffer bb = symbolBuffers.get(i);

                // Record index entry
                indexMap.put(symbol, new IndexEntry(dataOffset, bb.remaining()));

                // Write length-prefixed FlatBuffer
                ByteBuffer lenBuf = ByteBuffer.allocate(4);
                lenBuf.putInt(bb.remaining());
                lenBuf.flip();
                channel.write(lenBuf);
                channel.write(bb);

                dataOffset += 4 + bb.remaining();
            }

            // Go back to start to write index
            channel.position(0);
            ByteBuffer header = ByteBuffer.allocate(4);
            header.putInt(NUM_SYMBOLS);
            header.flip();
            channel.write(header);

            for (Map.Entry<String, IndexEntry> e : indexMap.entrySet()) {
                byte[] nameBytes = e.getKey().getBytes(StandardCharsets.UTF_8);
                if (nameBytes.length > Short.MAX_VALUE)
                    throw new RuntimeException("Symbol name too long");

                ByteBuffer buf = ByteBuffer.allocate(2 + nameBytes.length + 8 + 4);
                buf.putShort((short) nameBytes.length);
                buf.put(nameBytes);
                buf.putLong(e.getValue().offset);
                buf.putInt(e.getValue().length);
                buf.flip();
                channel.write(buf);
            }
        }

        System.out.println("Generated " + NUM_SYMBOLS + " symbols with index in " + FILE_NAME);
    }

    static class IndexEntry {
        long offset;
        int length;

        public IndexEntry(long offset, int length) {
            this.offset = offset;
            this.length = length;
        }
    }
}

