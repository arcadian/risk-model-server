packate fb.risk;

import com.google.flatbuffers.FlatBufferBuilder;
import quotes.SymbolData;

import java.io.IOException;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

public class FlatDBWriter {

    record IndexEntry(String symbol, int offset, int length) {}

    public static void writeDatabase(Path file, Map<String, float[]> symbolToPrices) throws IOException {

        List<String> symbols = new ArrayList<>(symbolToPrices.keySet());
        symbols.sort(String::compareTo); // consistent ordering (optional)

        try (FileChannel ch = FileChannel.open(file,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

            // reserve header space
            ByteBuffer hdr = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
            hdr.put("FLATDB02".getBytes(StandardCharsets.US_ASCII)); // 8 bytes
            hdr.putInt(symbols.size());
            hdr.putInt(0); // placeholder for index size
            hdr.flip();
            ch.write(hdr);

            List<IndexEntry> index = new ArrayList<>();
            List<ByteBuffer> flatBuffers = new ArrayList<>();

            int dataOffset = 16; // header ends at 16, index begins here initially

            // Build flatbuffers and store size for index
            for (String sym : symbols) {
                float[] prices = symbolToPrices.get(sym);

                FlatBufferBuilder fbb = new FlatBufferBuilder(256);
                int symOff = fbb.createString(sym);
                int pricesOff = SymbolData.createPricesVector(fbb, prices);
                int root = SymbolData.createSymbolData(fbb, symOff, pricesOff);
                fbb.finish(root);

                ByteBuffer bb = fbb.dataBuffer();
                int start = bb.position();
                int length = bb.remaining();

                ByteBuffer slice = bb.duplicate();
                slice.position(start);
                slice.limit(start + length);

                flatBuffers.add(slice);
                index.add(new IndexEntry(sym, -1, length)); // offset set after index region known
            }

            // compute index region size
            int indexSize = 0;
            for (IndexEntry e : index) {
                indexSize += 4 + e.symbol.length() + 4 + 4;
            }

            // rewrite index size in header
            ByteBuffer idxSizeBuf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
            idxSizeBuf.putInt(indexSize);
            idxSizeBuf.flip();
            ch.position(8 + 4);
            ch.write(idxSizeBuf);

            // write index
            int indexStart = 16;
            int cursor = indexStart;

            ch.position(indexStart);
            ByteBuffer idxBuf = ByteBuffer.allocate(indexSize).order(ByteOrder.LITTLE_ENDIAN);

            int currentOffset = indexStart + indexSize; // data region start

            for (int i = 0; i < index.size(); i++) {
                IndexEntry e = index.get(i);
                byte[] symBytes = e.symbol.getBytes(StandardCharsets.UTF_8);

                idxBuf.putInt(symBytes.length);
                idxBuf.put(symBytes);
                idxBuf.putInt(e.length);
                idxBuf.putInt(currentOffset);

                index.set(i, new IndexEntry(e.symbol, currentOffset, e.length));
                currentOffset += e.length;
            }
            idxBuf.flip();
            ch.write(idxBuf);

            // write data region
            ch.position(indexStart + indexSize);
            for (ByteBuffer bb : flatBuffers) {
                ch.write(bb);
            }
        }
    }
}

