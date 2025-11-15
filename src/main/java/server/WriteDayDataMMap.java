package server;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import com.google.flatbuffers.FlatBufferBuilder;
import fb.risk.SymbolEntry;
import fb.risk.DayData;

public class WriteDayDataMMAP {

    // adjust these constants if you want different sizes
    static final int NUM_SYMBOLS = 100;
    static final int NUM_FACTORS = 100;
    static final String OUT_FILE = "daydata.fb";

    public static void main(String[] args) throws Exception {
        FlatBufferBuilder builder = new FlatBufferBuilder(1024 * 1024);

        // We'll create NUM_SYMBOLS SymbolEntry tables and collect their offsets
        int[] symbolOffsets = new int[NUM_SYMBOLS];

        for (int i = 0; i < NUM_SYMBOLS; i++) {
            // deterministic symbol name: sym1..sym100
            String name = "sym" + (i + 1);
            int nameOffset = builder.createString(name);

            // create deterministic factor values for reproducibility
            double[] factors = new double[NUM_FACTORS];
            for (int f = 0; f < NUM_FACTORS; f++) {
                // example deterministic value, change as needed
                factors[f] = (i + 1) * 1000.0 + f * 0.01;
            }
            // create a double vector in the FlatBuffer
            int factorsOffset = SymbolEntry.createFactorsVector(builder, factors);

            // create SymbolEntry table
            SymbolEntry.startSymbolEntry(builder);
            SymbolEntry.addName(builder, nameOffset);
            SymbolEntry.addFactors(builder, factorsOffset);
            int symbolEntryOffset = SymbolEntry.endSymbolEntry(builder);

            symbolOffsets[i] = symbolEntryOffset;
        }

        // create vector of SymbolEntry tables for DayData
        int symbolsVector = DayData.createSymbolsVector(builder, symbolOffsets);

        // build root DayData
        DayData.startDayData(builder);
        DayData.addSymbols(builder, symbolsVector);
        int dayDataOffset = DayData.endDayData(builder);

        builder.finish(dayDataOffset); // finishes the buffer with root type

        // get byte[] of the finished buffer
        byte[] bytes = builder.sizedByteArray();
        long size = bytes.length;

        // write to memory-mapped file
        try (RandomAccessFile raf = new RandomAccessFile(OUT_FILE, "rw");
             FileChannel fc = raf.getChannel()) {

            // ensure file is the correct size
            raf.setLength(size);

            // map file into memory and write the bytes
            MappedByteBuffer mbb = fc.map(FileChannel.MapMode.READ_WRITE, 0, size);
            mbb.put(bytes);
            mbb.force(); // optional: force to disk
        }

        System.out.println("Wrote " + size + " bytes to " + OUT_FILE + " (memory-mapped).");
    }
}

