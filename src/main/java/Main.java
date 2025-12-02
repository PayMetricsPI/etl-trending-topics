import com.google.genai.Client;
import com.google.genai.types.GenerateContentResponse;
import infrastructure.csv.service.ReadCSVService;
import infrastructure.csv.service.WriteCSVService;
import infrastructure.s3.service.S3ClientFactory;
import infrastructure.s3.service.S3ReceivedService;
import infrastructure.s3.service.S3SendService;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class Main {
    public static void main(String[] args) throws IOException {
        String accessKey = System.getenv("ACCESS_KEY");
        String secretKey = System.getenv("SECRET_KEY");
        String sessionToken = System.getenv("SESSION_TOKEN");
        String bucketNameReceived = "bucket-guigo-raw";
        String bucketNameSend = "bucket-guigo-trusted";

        Path inputFolder = Path.of("src/main/resources/input-csv");
        Path outputFolder = Path.of("src/main/resources/output-csv");
        Region region = Region.US_EAST_1;

        try (S3Client s3Client = S3ClientFactory.createClient(accessKey, secretKey, sessionToken, region)) {

            if (!Files.exists(inputFolder)) Files.createDirectories(inputFolder);

            if (!Files.exists(outputFolder)) Files.createDirectories(outputFolder);


            S3ReceivedService s3ReceivedService = new S3ReceivedService(s3Client, bucketNameReceived);
            S3SendService s3SendService = new S3SendService(s3Client, bucketNameSend);
            ReadCSVService readCSVService = new ReadCSVService();
            WriteCSVService writeCSVService = new WriteCSVService();

            s3ReceivedService.processFiles(inputFolder);

            List<Path> csvFiles = readCSVService.getCSVFiles(inputFolder);

            List<String[]> allRows = new ArrayList<>();
            boolean headerAdded = false;

            for (Path csvFile : csvFiles) {
                System.out.println("Processando arquivo: " + csvFile.getFileName());
                List<String[]> rows = readCSVService.readCSV(csvFile);

                if (rows.isEmpty()) {
                    continue;
                }

                if (!headerAdded) {
                    allRows.add(rows.get(0));
                    headerAdded = true;
                }

                for (int i = 1; i < rows.size(); i++) {
                    String[] row = rows.get(i);
                    if (row[1].equals("<1")) row[1] = "1";
                    allRows.add(row);
                }
            }

            System.setProperty("GOOGLE_API_KEY", "AIzaSyDFHzGRkieX1xMoa6LTz03p__1lS2DjZP4");

            Client client = new Client();

            GenerateContentResponse response =
                    client.models.generateContent(
                            "gemini-3-pro-preview",
                            "A seguir estão datas sazonais no Brasil:\n" +
                                    "\n" +
                                    "Jan/2026: 05, 12, 24, 30, 31\n" +
                                    "Fev/2026: 16\n" +
                                    "Mar/2026: 28\n" +
                                    "Abr/2026: 02, 10, 23, 30\n" +
                                    "Jun/2026: 06, 07, 18, 19, 20\n" +
                                    "Jul/2026: 09, 14\n" +
                                    "Out/2026: 02, 09, 23, 24\n" +
                                    "Nov/2026: 21, 22, 26\n" +
                                    "Dez/2026: 18, 20, 25, 26, 27\n" +
                                    "\n" +
                                    "Para CADA data acima, identifique somente os eventos sazonais mais relevantes no Brasil que acontecem no mesmo dia ou que antecedem a data. Os eventos permitidos são exclusivamente:\n" +
                                    "\n" +
                                    "[\"amazon day\", \"blackfriday\", \"dia das crianças\", \"cybermonday\", \"carnaval\", \"halloween\", \"aulas\", \"pascoa\", \"natal\", \"dia das maes\", \"dia dos namorados\", \"festa junina\", \"reveillon\"]\n" +
                                    "\n" +
                                    "Para cada data encontrada, retorne uma lista de objetos seguindo EXATAMENTE este formato:\n" +
                                    "\n" +
                                    "[\n" +
                                    "  {\n" +
                                    "    \"data\": \"YYYY-MM-DD\",\n" +
                                    "    \"eventos\": [\n" +
                                    "      {\n" +
                                    "        \"nome\": \"string\",\n" +
                                    "        \"data_referencia\": \"YYYY-MM-DD\",\n" +
                                    "        \"impacto_hardware\": \"string curta\",\n" +
                                    "        \"recomendacao_ti\": \"string curta\"\n" +
                                    "      }\n" +
                                    "    ]\n" +
                                    "  }\n" +
                                    "]\n" +
                                    "\n" +
                                    "Regras OBRIGATÓRIAS:\n" +
                                    "- Responda SOMENTE com JSON válido.\n" +
                                    "- Não inclua comentários, explicações, texto antes ou depois.\n" +
                                    "- Não coloque vírgula sobrando no final.\n" +
                                    "- Todos os campos devem existir mesmo que vazios.\n" +
                                    "- Se nenhuma data tiver evento, retornar um array vazio: [].\n" +
                                    "Lembre que isso é direcionado para uma persona estratégica, que trabalha com monitoramento de hardware em servidores de pagamento de e-commerce" +
                                    "\n" +
                                    "Agora gere a resposta final em JSON puro.\n",
                            null
                    );

            String respostaJson = response.text();

            Path tempJson = Files.createTempFile("eventos_2026_gemini", ".json");
            Files.writeString(tempJson, respostaJson);

            S3SendService s3Service = new S3SendService(s3Client, "bucket-guigo-client");
            s3Service.uploadFile(tempJson);


            Path outputFile = outputFolder.resolve("seasonal-terms.csv");
            writeCSVService.writeCSV(outputFile, allRows);

            System.out.println("Enviando para o bucket trusted");
            s3SendService.uploadFile(outputFile);

        }
    }
}
