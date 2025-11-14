package server;

import java.io.File;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.ByteOrder;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import Risk.DayData;
import Risk.SymbolEntry;

import java.util.concurrent.TimeUnit;

public class MemoryMappedRiskServer {

    static class SymbolKey {
        String symbol;
        String model;
        String date;
        public SymbolKey(String symbol, String model, String date) {
            this.symbol = symbol;
            this.model = model;
            this.date = date;
        }
        @Override
        public int hashCode() { return symbol.hashCode() ^ model.hashCode() ^ date.hashCode(); }
        @Override
        public boolean equals(Object o) {
            if (!(o instanceof SymbolKey)) return false;
            SymbolKey k = (SymbolKey) o;
            return symbol.equals(k.symbol) && model.equals(k.model) && date.equals(k.date);
        }
    }

    // Cache: key = symbol/model/date, value = ByteBuffer slice (zero-copy)
    static Cache<SymbolKey, ByteBuffer> symbolCache = Caffeine.newBuilder()
            .maximumSize(10000)
            .expireAfterAccess(30, TimeUnit.MINUTES)
            .build();

    // Lazy loader
    static ByteBuffer loadSymbolSlice(SymbolKey key, File mmapFile) throws Exception {
        MappedByteBuffer mmap = new RandomAccessFile(mmapFile, "r")
                .getChannel().map(FileChannel.MapMode.READ_ONLY, 0, mmapFile.length());
        mmap.order(ByteOrder.LITTLE_ENDIAN);

        DayData day = DayData.getRootAsDayData(mmap);

        for (int i = 0; i < day.symbolsLength(); i++) {
            SymbolEntry s = day.symbols(i);
            if (s.name().equals(key.symbol)) {
                ByteBuffer slice = s.factorsAsByteBuffer(); // zero-copy
                return slice;
            }
        }
        throw new IllegalArgumentException("Symbol not found: " + key.symbol);
    }

    public static ByteBuffer getSymbolSlice(SymbolKey key, File mmapFile) {
        return symbolCache.get(key, k -> {
            try {
                return loadSymbolSlice(k, mmapFile);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static void main(String[] args) throws Exception {
        File mmapFile = new File("day1.bin");
        SymbolKey key = new SymbolKey("AAPL", "default", "2025-11-14");
        ByteBuffer slice = getSymbolSlice(key, mmapFile);

        System.out.println("Slice remaining bytes: " + slice.remaining());
        while (slice.hasRemaining()) {
            System.out.print(slice.getDouble() + " ");
        }
    }
}

