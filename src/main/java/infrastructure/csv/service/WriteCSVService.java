package infrastructure.csv.service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class WriteCSVService {
    public void writeCSV(Path outputFile, List<String[]> lines) throws IOException {

        String[] header = lines.get(0);

        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)) {
            bufferedWriter.write(String.join(",", header));
            bufferedWriter.newLine();

            for (int i = 1; i < lines.size(); i++) {
                String[] columns = lines.get(i);
                bufferedWriter.write(String.join(",", columns));
                bufferedWriter.newLine();
            }
        }
    }
}
