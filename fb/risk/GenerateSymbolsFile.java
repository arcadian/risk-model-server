import fb.risk.SymbolData;
import com.google.flatbuffers.FlatBufferBuilder;

import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class GenerateSymbolsFile {

    static final int NUM_SYMBOLS = 100;
    static final int NUM_FACTORS = 100;
    static final String FILE_NAME = "symbols_combined.bin";

    public static void main(String[] args) throws Exception {
        try (FileOutputStream fos = new FileOutputStream(FILE_NAME);
             FileChannel channel = fos.getChannel()) {

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

                // Write length prefix
                ByteBuffer lenBuf = ByteBuffer.allocate(4);
                lenBuf.putInt(bb.remaining());
                lenBuf.flip();
                channel.write(lenBuf);

                // Write FlatBuffer bytes
                channel.write(bb);
            }
        }

        System.out.println("Generated " + NUM_SYMBOLS + " symbols into " + FILE_NAME);
    }
}

