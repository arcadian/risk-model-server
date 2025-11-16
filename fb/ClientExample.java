import fb.risk.SymbolData;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class ClientExample {

    public static void main(String[] args) throws Exception {
        try (SymbolStore store = new SymbolStore("symbols_combined.bin")) {

            // Example list of symbols to read
            List<String> symbolsToRead = Arrays.asList("sym1", "sym42", "sym77");

            for (String sym : symbolsToRead) {
                ByteBuffer slice = store.get(sym);
                if (slice == null) {
                    System.out.println("Symbol not found: " + sym);
                    continue;
                }

                SymbolData data = SymbolData.getRootAsSymbolData(slice);
                System.out.println("Symbol: " + data.name());
                System.out.println("First factor: " + data.factors(0));
            }
        }
    }
}

