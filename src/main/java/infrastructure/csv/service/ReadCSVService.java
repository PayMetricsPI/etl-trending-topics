package infrastructure.csv.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class ReadCSVService {

    public List<Path> getCSVFiles(Path folderPath) throws IOException {
        List<Path> csvFiles = new ArrayList<>();

        try (DirectoryStream<Path> directory = Files.newDirectoryStream(folderPath, "*.csv")) {
            for (Path path : directory) {
                csvFiles.add(path);
            }
        }
        return csvFiles;
    }

    public List<String[]> readCSV(Path filePath) throws IOException {
        List<String[]> lines = new ArrayList<>();

        try (BufferedReader bufferedReader = Files.newBufferedReader(filePath)) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] columns = line.split(",");
                lines.add(columns);
            }
        }
        return lines;
    }
}
