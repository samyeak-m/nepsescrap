import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.io.IOException;
import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Random;

public class NepseScraper {

    private static final String DB_URL = "jdbc:mysql://localhost:3306/nepse_data";
    private static final String DB_USER = "root"; // Replace with your actual MySQL username
    private static final String DB_PASS = "";   // Replace with your actual MySQL password

    private static final String[] USER_AGENTS = {
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/117.0"
            // Add more user agents here
    };

    public static void main(String[] args) {
        try {
            String historicalDataURL = "https://nepsealpha.com.np/trading/1/history";
            String currentDataURL = "https://www.sharesansar.com/today-share-price";

            createTablesIfNotExists();

//            scrapeAndStoreData(fetchDocument(historicalDataURL), "historical_data", true);
            scrapeAndStoreData(fetchDocument(currentDataURL), "live_data", false);

        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }

    private static Document fetchDocument(String url) throws IOException {
        Random random = new Random();
        String userAgent = USER_AGENTS[random.nextInt(USER_AGENTS.length)];
        return Jsoup.connect(url).userAgent(userAgent).get();
    }

    private static void createTablesIfNotExists() throws SQLException {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
             Statement stmt = conn.createStatement()) {

            String historicalTableSQL = "CREATE TABLE IF NOT EXISTS historical_data (" +
                    "id INT AUTO_INCREMENT PRIMARY KEY," +
                    "date DATE," +
                    "symbol VARCHAR(255)," +
                    "open DOUBLE," +
                    "high DOUBLE," +
                    "low DOUBLE," +
                    "close DOUBLE," +
                    "volume BIGINT," +
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                    ")";
            stmt.executeUpdate(historicalTableSQL);

            String liveTableSQL = "CREATE TABLE IF NOT EXISTS live_data (" +
                    "id INT AUTO_INCREMENT PRIMARY KEY," +
                    "symbol VARCHAR(255)," +
                    "ltp DOUBLE," +
                    "point_change DOUBLE," +
                    "percent_change DOUBLE," +
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                    ")";
            stmt.executeUpdate(liveTableSQL);
        }
    }

    private static void scrapeAndStoreData(Document doc, String tableName, boolean isHistorical) throws SQLException {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
            for (Element row : doc.select("table.table tr")) {
                Elements cells = row.select("td");

                // Check if the row has enough cells AND the second cell is numeric
                if ((isHistorical && cells.size() >= 7 && cells.get(2).text().matches("-?\\d+(\\.\\d+)?"))
                        || (!isHistorical && cells.size() >= 5 && cells.get(1).text().matches("-?\\d+(\\.\\d+)?"))) {

                    if (isHistorical) {
                        // Extract and insert/update historical data
                        String dateStr = cells.get(0).text();
                        LocalDate date = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                        String symbol = cells.get(1).text();
                        double open = Double.parseDouble(cells.get(3).text().replace(",", ""));
                        double high = Double.parseDouble(cells.get(4).text().replace(",", ""));
                        double low = Double.parseDouble(cells.get(5).text().replace(",", ""));
                        double close = Double.parseDouble(cells.get(6).text().replace(",", ""));
                        long volume = Long.parseLong(cells.get(8).text().replace(",", ""));

                        insertOrUpdateData(conn, "historical_data", date, symbol, open, high, low, close, volume);
                    } else {
                        // Extract and update live data
                        String symbol = cells.get(0).text();
                        double ltp = Double.parseDouble(cells.get(1).text().replace(",", ""));
                        double pointChange = Double.parseDouble(cells.get(2).text().replace(",", ""));
                        double percentChange = Double.parseDouble(cells.get(3).text().replace(",", "").replace("%", "")); // Remove % sign

                        insertOrUpdateData(conn, "historical_data", LocalDate.now(), symbol, ltp, ltp, ltp, ltp, 0);
                        insertOrUpdateData(conn, "live_data", null, symbol, ltp, pointChange, percentChange, 0, 0);
                    }
                }
            }
        }
    }
    // update or insert query
    private static void insertOrUpdateData(Connection conn, String tableName, LocalDate date, String symbol,
                                           double open, double high, double low, double close, long volume) throws SQLException {
        String insertSql = "INSERT INTO " + tableName + " (date, symbol, open, high, low, close, volume, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, NOW())";
        String updateSql = "UPDATE " + tableName + " SET open = ?, high = ?, low = ?, close = ?, volume = ?, created_at = NOW() WHERE date = ? AND symbol = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(tableName.equals("historical_data") ? insertSql : updateSql)) {
            int parameterIndex = 1;
            if (tableName.equals("historical_data")) {
                pstmt.setObject(parameterIndex++, date);
            }
            pstmt.setString(parameterIndex++, symbol);
            pstmt.setDouble(parameterIndex++, open);
            pstmt.setDouble(parameterIndex++, high);
            pstmt.setDouble(parameterIndex++, low);
            pstmt.setDouble(parameterIndex++, close);
            pstmt.setLong(parameterIndex++, volume);

            if (tableName.equals("live_data")) {
                pstmt.setDouble(parameterIndex++, open);
                pstmt.setDouble(parameterIndex++, high);
                pstmt.setDouble(parameterIndex++, low);
                pstmt.setDouble(parameterIndex++, close);
                pstmt.setLong(parameterIndex++, volume);
                pstmt.setObject(parameterIndex++, LocalDate.now());
                pstmt.setString(parameterIndex, symbol);
            }

            pstmt.executeUpdate();

            System.out.println("Data for " + symbol + " " + (tableName.equals("historical_data") ? "inserted/updated into historical_data" : "updated in live_data"));
        }
    }

}
