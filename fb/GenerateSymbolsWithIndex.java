import fb.risk.SymbolData;
import com.google.flatbuffers.FlatBufferBuilder;

import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

public class GenerateSymbolsWithIndex {

    static final int NUM_SYMBOLS = 100;
    static final int NUM_FACTORS = 100;
    static final String FILE_NAME = "symbols_indexed.bin";

    public static void main(String[] args) throws Exception {

        Map<String, IndexEntry> indexMap = new LinkedHashMap<>();

        try (FileOutputStream fos = new FileOutputStream(FILE_NAME);
             FileChannel channel = fos.getChannel()) {

            // Reserve space for index; we’ll write it after data section
            channel.position(0);

            // Temporary list to hold FlatBuffers for writing
            ByteBuffer[] buffers = new ByteBuffer[NUM_SYMBOLS];

            // 1️⃣ Build FlatBuffers in memory
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

                buffers[i] = builder.dataBuffer();
            }

            // 2️⃣ Write data section with length-prefixed FlatBuffers
            long dataSectionStart = 0;
            for (int i = 0; i < NUM_SYMBOLS; i++) {
                ByteBuffer bb = buffers[i];
                long symbolOffset = channel.position(); // offset for index BEFORE writing length

                // write length prefix
                ByteBuffer lenBuf = ByteBuffer.allocate(4).putInt(bb.remaining());
                lenBuf.flip();
                channel.write(lenBuf);

                // write FlatBuffer bytes
                channel.write(bb);

                // store index entry
                indexMap.put("sym" + i, new IndexEntry(symbolOffset, bb.remaining()));
            }

            // 3️⃣ Write index at the start of the file
            channel.position(0);

            // number of symbols
            ByteBuffer header = ByteBuffer.allocate(4).putInt(NUM_SYMBOLS);
            header.flip();
            channel.write(header);

            // write index entries
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
        long offset;  // offset points to start of 4-byte length prefix
        int length;   // length of FlatBuffer (excluding length prefix)
        IndexEntry(long offset, int length) { this.offset = offset; this.length = length; }
    }
}

