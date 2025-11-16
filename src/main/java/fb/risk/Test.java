package fb.risk;

public class Test{

public static void main(String[] args) throws Exception {
    Path file = Paths.get("symbols.bin");

    Map<String, float[]> data = Map.of(
            "AAPL", new float[]{1,2,3},
            "GOOG", new float[]{10,20},
            "MSFT", new float[]{7,8,9,10}
    );

    FlatDBWriter.writeDatabase(file, data);

    FlatDBReader reader = new FlatDBReader(file);
    SymbolData d = reader.get("GOOG");

    System.out.println(d.symbol());
    System.out.println(d.prices(0));
    System.out.println(d.prices(1));
}


}
