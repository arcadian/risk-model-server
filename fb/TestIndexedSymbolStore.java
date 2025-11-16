import fb.risk.SymbolData;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class TestIndexedSymbolStore {

    public static void main(String[] args) throws Exception {

        IndexedSymbolStore store = new IndexedSymbolStore("symbols_indexed.bin");

        List<String> symbolsToRead = Arrays.asList("sym0", "sym42", "sym77");

        for (String sym : symbolsToRead) {
            ByteBuffer slice = store.get(sym);

            if (slice == null) {
                System.out.println("Symbol not found: " + sym);
                continue;
            }

            SymbolData symbolData = SymbolData.getRootAsSymbolData(slice);

            System.out.println("Symbol: " + symbolData.name());
            System.out.println("Number of factors: " + symbolData.factorsLength());
            System.out.println("First factor: " + symbolData.factors(0));
            System.out.println("---");
        }
    }
}

