import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Base64;

public class liveData {

    private static final String DB_URL = "jdbc:mysql://localhost:3306/nepse_test";
    private static final String DB_USER = "root";
    private static final String DB_PASS = "";
    private static final long INTERVAL = 60000;
    private static final LocalTime START_OF_DAY = LocalTime.of(11, 00);
    private static final LocalTime END_OF_DAY = LocalTime.of(15, 01);
    private static final ZoneId NEPAL_ZONE = ZoneId.of("Asia/Kathmandu");

    private static String lastHash = "";
    private static LocalDate lastCheckedDate = LocalDate.now();

    public static void main(String[] args) {
        try {
            createTableIfNotExists();
            createTransactionTableIfNotExists();
            System.out.println("Tables checked/created successfully.");
        } catch (SQLException e) {
            System.err.println("Error creating tables: " + e.getMessage());
            return;
        }

        while (true) {
            try {
                ZonedDateTime nowNepal = ZonedDateTime.now(NEPAL_ZONE);
                LocalTime now = nowNepal.toLocalTime();
                LocalDate today = nowNepal.toLocalDate();
                DayOfWeek dayOfWeek = today.getDayOfWeek();

                if (dayOfWeek == DayOfWeek.FRIDAY || dayOfWeek == DayOfWeek.SATURDAY) {
                    System.out.println("Market is closed on Friday and Saturday. Sleeping until Sunday.");
                    Thread.sleep(getSleepDurationUntilSunday());
                    continue;
                }

                if (!today.equals(lastCheckedDate)) {
                    clearTransactionTable();
                    lastCheckedDate = today;
                }

                if (now.isBefore(START_OF_DAY) || now.isAfter(END_OF_DAY)) {
                    System.out.println("Market is closed. Sleeping until next check.");
                    Thread.sleep(getSleepDuration());
                    continue;
                }

                String currentDataURL = "https://www.sharesansar.com/live-trading";
                String content = fetchData(currentDataURL);
                String currentHash = generateHash(content);

                if (!currentHash.equals(lastHash)) {
                    scrapeAndStoreData(content);
                    lastHash = currentHash;
                    LocalDateTime nowDateTime = LocalDateTime.now();
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    String formattedNow = nowDateTime.format(formatter);
                    System.out.println("Data updated successfully at " + formattedNow);
                } else {
                    System.out.println("Data remains the same. Skipping database update.");
                }
            } catch (Exception e) {
                System.err.println("Error fetching or storing data: " + e.getMessage());
            }

            try {
                Thread.sleep(INTERVAL);
            } catch (InterruptedException e) {
                System.err.println("Thread interrupted: " + e.getMessage());
                Thread.currentThread().interrupt();
            }
        }
    }

    private static long getSleepDuration() {
        LocalTime now = LocalTime.now();
        LocalTime nextStart = now.isBefore(START_OF_DAY) ? START_OF_DAY : START_OF_DAY.plusHours(24);
        return java.time.Duration.between(now, nextStart).toMillis();
    }

    private static long getSleepDurationUntilSunday() {
        LocalDate now = LocalDate.now();
        LocalDate nextSunday = now.plusDays(7 - now.getDayOfWeek().getValue());
        LocalDateTime nextStart = LocalDateTime.of(nextSunday, START_OF_DAY);
        return java.time.Duration.between(LocalDateTime.now(), nextStart).toMillis();
    }

    private static String fetchData(String urlStr) throws IOException {
        StringBuilder result = new StringBuilder();
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("User-Agent", "Mozilla/5.0");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                result.append(line);
            }
        }
        return result.toString();
    }

    private static String generateHash(String content) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(content.getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(hash);
    }

    private static void createTableIfNotExists() throws SQLException {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
             Statement stmt = conn.createStatement()) {

            String liveTableSQL = "CREATE TABLE IF NOT EXISTS live_data (" +
                    "id INT AUTO_INCREMENT PRIMARY KEY," +
                    "date DATE," +
                    "symbol VARCHAR(255)," +
                    "ltp DOUBLE(10,2)," +
                    "pointChange VARCHAR(20)," +
                    "perChange VARCHAR(20)," +
                    "open DOUBLE(10,2)," +
                    "high DOUBLE(10,2)," +
                    "low DOUBLE(10,2)," +
                    "prev_close DOUBLE(10,2)," +
                    "UNIQUE KEY unique_date_symbol (date, symbol)" +
                    ")";
            stmt.executeUpdate(liveTableSQL);
        }
    }

    private static void createTransactionTableIfNotExists() throws SQLException {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
             Statement stmt = conn.createStatement()) {

            String transactionTableSQL = "CREATE TABLE IF NOT EXISTS transaction_data (" +
                    "id INT AUTO_INCREMENT PRIMARY KEY," +
                    "timestamp TIMESTAMP," +
                    "symbol VARCHAR(255)," +
                    "ltp DOUBLE(10,2)," +
                    "pointChange VARCHAR(20)," +
                    "perChange VARCHAR(20)," +
                    "open DOUBLE(10,2)," +
                    "high DOUBLE(10,2)," +
                    "low DOUBLE(10,2)," +
                    "prev_close DOUBLE(10,2)" +
                    ")";
            stmt.executeUpdate(transactionTableSQL);
        }
    }

    private static void clearTransactionTable() {
        System.out.println("Starting to clear transaction table.");
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
            LocalDate today = LocalDate.now();
            System.out.println("Today's date: " + today);

            // Log current rows in the table before deletion
            String selectSql = "SELECT * FROM transaction_data";
            try (Statement selectStmt = conn.createStatement();
                 ResultSet rs = selectStmt.executeQuery(selectSql)) {
                while (rs.next()) {
                    System.out.println("Row before delete: " +
                            "ID: " + rs.getInt("id") +
                            ", Timestamp: " + rs.getTimestamp("timestamp") +
                            ", Symbol: " + rs.getString("symbol"));
                }
            }

            System.out.println("Clearing transaction data before: " + today);

            String deleteSql = "DELETE FROM transaction_data WHERE DATE(timestamp) < ?";
            try (PreparedStatement pstmt = conn.prepareStatement(deleteSql)) {
                pstmt.setDate(1, Date.valueOf(today));
                int rowsDeleted = pstmt.executeUpdate();
                System.out.println("Transaction table cleared for new day. Rows deleted: " + rowsDeleted);
            }

            // Log current rows in the table after deletion
            try (Statement selectStmt = conn.createStatement();
                 ResultSet rs = selectStmt.executeQuery(selectSql)) {
                while (rs.next()) {
                    System.out.println("Row after delete: " +
                            "ID: " + rs.getInt("id") +
                            ", Timestamp: " + rs.getTimestamp("timestamp") +
                            ", Symbol: " + rs.getString("symbol"));
                }
            }
        } catch (SQLException e) {
            System.err.println("Error clearing transaction table: " + e.getMessage());
        }
    }

    private static void scrapeAndStoreData(String content) throws SQLException {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
            Document doc = Jsoup.parse(content);
            for (Element row : doc.select("table.table tr")) {
                Elements cells = row.select("td");

                if (cells.size() < 2) {
                    System.out.println("Skipping row with insufficient columns: " + row.text());
                    continue;
                }

                String symbol = cells.get(1).text().trim();
                if (symbol.isEmpty()) {
                    System.out.println("Skipping row with empty symbol: " + row.text());
                    continue;
                }

                while (cells.size() < 21) {
                    cells.add(new Element("td").text("0"));
                }

                try {
                    LocalDate date = LocalDate.now();
                    double ltp = parseDouble(cells.get(2).text());
                    String pointChange = cells.get(3).text();
                    String perChange = cells.get(4).text();
                    double open = parseDouble(cells.get(5).text());
                    double high = parseDouble(cells.get(6).text());
                    double low = parseDouble(cells.get(7).text());
                    double prevClose = parseDouble(cells.get(9).text());

                    // Insert or update in live_data table
                    String checkSql = "SELECT COUNT(*) FROM live_data WHERE date = ? AND symbol = ?";
                    try (PreparedStatement checkStmt = conn.prepareStatement(checkSql)) {
                        checkStmt.setObject(1, date);
                        checkStmt.setString(2, symbol);
                        try (ResultSet rs = checkStmt.executeQuery()) {
                            if (rs.next() && rs.getInt(1) > 0) {
                                String updateSql = "UPDATE live_data SET ltp = ?, pointChange = ?, perChange = ?, open = ?, high = ?, low = ?, prev_close = ? WHERE date = ? AND symbol = ?";
                                try (PreparedStatement updateStmt = conn.prepareStatement(updateSql)) {
                                    updateStmt.setDouble(1, ltp);
                                    updateStmt.setString(2, pointChange);
                                    updateStmt.setString(3, perChange);
                                    updateStmt.setDouble(4, open);
                                    updateStmt.setDouble(5, high);
                                    updateStmt.setDouble(6, low);
                                    updateStmt.setDouble(7, prevClose);
                                    updateStmt.setObject(8, date);
                                    updateStmt.setString(9, symbol);
                                    updateStmt.executeUpdate();
                                }
                            } else {
                                String insertSql = "INSERT INTO live_data (date, symbol, ltp, pointChange, perChange, open, high, low, prev_close) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
                                try (PreparedStatement insertStmt = conn.prepareStatement(insertSql)) {
                                    insertStmt.setObject(1, date);
                                    insertStmt.setString(2, symbol);
                                    insertStmt.setDouble(3, ltp);
                                    insertStmt.setString(4, pointChange);
                                    insertStmt.setString(5, perChange);
                                    insertStmt.setDouble(6, open);
                                    insertStmt.setDouble(7, high);
                                    insertStmt.setDouble(8, low);
                                    insertStmt.setDouble(9, prevClose);
                                    insertStmt.executeUpdate();
                                }
                            }
                        }
                    }

                    // Insert into transaction_data table
                    String transactionSql = "INSERT INTO transaction_data (timestamp, symbol, ltp, pointChange, perChange, open, high, low, prev_close) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
                    try (PreparedStatement transactionStmt = conn.prepareStatement(transactionSql)) {
                        transactionStmt.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now()));
                        transactionStmt.setString(2, symbol);
                        transactionStmt.setDouble(3, ltp);
                        transactionStmt.setString(4, pointChange);
                        transactionStmt.setString(5, perChange);
                        transactionStmt.setDouble(6, open);
                        transactionStmt.setDouble(7, high);
                        transactionStmt.setDouble(8, low);
                        transactionStmt.setDouble(9, prevClose);
                        transactionStmt.executeUpdate();
                    }

                } catch (Exception e) {
                    System.err.println("Error processing row: " + row.text() + " Error: " + e.getMessage());
                }
            }
        }
    }

    private static double parseDouble(String text) {
        try {
            return Double.parseDouble(text.replaceAll("[^\\d.]", ""));
        } catch (NumberFormatException e) {
            System.err.println("Error parsing double from text: " + text + ". Returning 0.0");
            return 0.0;
        }
    }
}
