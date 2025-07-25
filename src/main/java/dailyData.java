import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class dailyData {

    private static final String DB_URL = "jdbc:mysql://localhost:3306/nepse_test";
    private static final String DB_USER = "root";
    private static final String DB_PASS = "";
    private static final long INTERVAL = 60000;
    private static final LocalTime START_OF_DAY = LocalTime.of(15, 00);
    private static final LocalTime END_OF_DAY = LocalTime.of(15, 02);

    private static final ZoneId NEPAL_ZONE = ZoneId.of("Asia/Kathmandu");

    private static String lastHash = "";
    private static final String LOG_FILE_PATH = "java";

    public static void main(String[] args) {
        try (PrintWriter logWriter = new PrintWriter(new FileWriter(LOG_FILE_PATH, true))) {
            try {
                createTableIfNotExists();
                logWriter.println("Table checked/created successfully.");
            } catch (SQLException e) {
                logWriter.println("Error creating table: " + e.getMessage());
                return;
            }

            while (true) {
                try {

                    ZonedDateTime nowNepal = ZonedDateTime.now(NEPAL_ZONE);
                    LocalTime now = nowNepal.toLocalTime();
                    LocalDate today = nowNepal.toLocalDate();
                    DayOfWeek dayOfWeek = today.getDayOfWeek();

                    if (dayOfWeek == DayOfWeek.FRIDAY || dayOfWeek == DayOfWeek.SATURDAY) {
                        logWriter.println("Market is closed on Friday and Saturday. Sleeping until Sunday.");
                        logWriter.flush();
                        Thread.sleep(getSleepDurationUntilSunday());
                        continue;
                    }

                    if (now.isBefore(START_OF_DAY) || now.isAfter(END_OF_DAY)) {
                        if (now.isAfter(END_OF_DAY)) {
                            storeLastUpdateOfTheDay();
                            logWriter.println("Last update of the day recorded at " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                            lastHash = "";
                        }
                        logWriter.println("Market is closed. Sleeping until next check.");
                        logWriter.flush();
                        Thread.sleep(getSleepDuration());
                        continue;
                    }

                    String currentDataURL = "https://www.sharesansar.com/today-share-price";
                    String content = fetchData(currentDataURL);
                    String currentHash = generateHash(content);

                    if (!currentHash.equals(lastHash)) {
                        scrapeAndStoreData(content);
                        lastHash = currentHash;
                        LocalDateTime nowDateTime = LocalDateTime.now();
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                        String formattedNow = nowDateTime.format(formatter);
                        logWriter.println("Data updated successfully at " + formattedNow);
                    } else {
                        logWriter.println("Data remains the same. Skipping database update.");
                    }
                } catch (Exception e) {
                    logWriter.println("Error fetching or storing data: " + e.getMessage());
                }

                try {
                    Thread.sleep(INTERVAL);
                } catch (InterruptedException e) {
                    logWriter.println("Thread interrupted: " + e.getMessage());
                    Thread.currentThread().interrupt();
                }
                logWriter.flush();
            }
        } catch (IOException e) {
            System.err.println("Error opening log file: " + e.getMessage());
        }
    }

    private static long getSleepDuration() {
        LocalTime now = LocalTime.now();
        LocalTime nextStart = now.isBefore(START_OF_DAY) ? START_OF_DAY : START_OF_DAY.plusHours(24);
        return now.isAfter(nextStart) ? java.time.Duration.between(nextStart, now).toMillis() : java.time.Duration.between(now, nextStart).toMillis();
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

            String dailyTableSQL = "CREATE TABLE IF NOT EXISTS daily_data (" +
                    "id INT AUTO_INCREMENT PRIMARY KEY," +
                    "date DATE," +
                    "symbol VARCHAR(255)," +
                    "conf DECIMAL(10,2)," +
                    "open DECIMAL(10,2)," +
                    "high DECIMAL(10,2)," +
                    "low DECIMAL(10,2)," +
                    "close DECIMAL(10,2)," +
                    "vwap VARCHAR(20)," +
                    "vol DECIMAL(20,2)," +
                    "prev_close DECIMAL(10,2)," +
                    "turnover DECIMAL(20,2)," +
                    "trans INT," +
                    "diff VARCHAR(20)," +
                    "`range` DECIMAL(10,2)," +
                    "diff_perc VARCHAR(20)," +
                    "range_perc DECIMAL(10,2)," +
                    "vwap_perc VARCHAR(20)," +
                    "days_120 DECIMAL(10,2)," +
                    "days_180 DECIMAL(10,2)," +
                    "weeks_52_high DECIMAL(10,2)," +
                    "weeks_52_low DECIMAL(10,2)," +
                    "UNIQUE KEY unique_date_symbol (date, symbol)" +
                    ")";
            stmt.executeUpdate(dailyTableSQL);
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
                    double conf = parseDouble(cells.get(2).text());
                    double open = parseDouble(cells.get(3).text());
                    double high = parseDouble(cells.get(4).text());
                    double low = parseDouble(cells.get(5).text());
                    double close = parseDouble(cells.get(6).text());
                    String vwap = cells.get(7).text();
                    double vol = parseDouble(cells.get(8).text());
                    double prevClose = parseDouble(cells.get(9).text());
                    double turnover = parseDouble(cells.get(10).text());
                    int trans = parseInt(cells.get(11).text());
                    String diff = cells.get(12).text();
                    double range = parseDouble(cells.get(13).text());
                    String diffPerc = cells.get(14).text();
                    double rangePerc = parseDouble(cells.get(15).text());
                    String vwapPerc = cells.get(16).text();
                    double days120 = parseDouble(cells.get(17).text());
                    double days180 = parseDouble(cells.get(18).text());
                    double weeks52High = parseDouble(cells.get(19).text());
                    double weeks52Low = parseDouble(cells.get(20).text());

                    String checkSql = "SELECT COUNT(*) FROM daily_data WHERE symbol = ?";
                    try (PreparedStatement checkStmt = conn.prepareStatement(checkSql)) {
                        checkStmt.setString(1, symbol);
                        try (ResultSet rs = checkStmt.executeQuery()) {
                            if (rs.next() && rs.getInt(1) > 0) {
                                String updateSql = "UPDATE daily_data SET date = ?, conf = ?, open = ?, high = ?, low = ?, close = ?, vwap = ?, vol = ?, prev_close = ?, turnover = ?, trans = ?, diff = ?, `range` = ?, diff_perc = ?, range_perc = ?, vwap_perc = ?, days_120 = ?, days_180 = ?, weeks_52_high = ?, weeks_52_low = ? WHERE symbol = ?";
                                try (PreparedStatement updateStmt = conn.prepareStatement(updateSql)) {
                                    updateStmt.setDate(1, Date.valueOf(date));
                                    updateStmt.setDouble(2, conf);
                                    updateStmt.setDouble(3, open);
                                    updateStmt.setDouble(4, high);
                                    updateStmt.setDouble(5, low);
                                    updateStmt.setDouble(6, close);
                                    updateStmt.setString(7, vwap);
                                    updateStmt.setDouble(8, vol);
                                    updateStmt.setDouble(9, prevClose);
                                    updateStmt.setDouble(10, turnover);
                                    updateStmt.setInt(11, trans);
                                    updateStmt.setString(12, diff);
                                    updateStmt.setDouble(13, range);
                                    updateStmt.setString(14, diffPerc);
                                    updateStmt.setDouble(15, rangePerc);
                                    updateStmt.setString(16, vwapPerc);
                                    updateStmt.setDouble(17, days120);
                                    updateStmt.setDouble(18, days180);
                                    updateStmt.setDouble(19, weeks52High);
                                    updateStmt.setDouble(20, weeks52Low);
                                    updateStmt.setString(21, symbol);
                                    updateStmt.executeUpdate();
                                }
                            } else {
                                String insertSql = "INSERT INTO daily_data (date, symbol, conf, open, high, low, close, vwap, vol, prev_close, turnover, trans, diff, `range`, diff_perc, range_perc, vwap_perc, days_120, days_180, weeks_52_high, weeks_52_low) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                                try (PreparedStatement insertStmt = conn.prepareStatement(insertSql)) {
                                    insertStmt.setDate(1, Date.valueOf(date));
                                    insertStmt.setString(2, symbol);
                                    insertStmt.setDouble(3, conf);
                                    insertStmt.setDouble(4, open);
                                    insertStmt.setDouble(5, high);
                                    insertStmt.setDouble(6, low);
                                    insertStmt.setDouble(7, close);
                                    insertStmt.setString(8, vwap);
                                    insertStmt.setDouble(9, vol);
                                    insertStmt.setDouble(10, prevClose);
                                    insertStmt.setDouble(11, turnover);
                                    insertStmt.setInt(12, trans);
                                    insertStmt.setString(13, diff);
                                    insertStmt.setDouble(14, range);
                                    insertStmt.setString(15, diffPerc);
                                    insertStmt.setDouble(16, rangePerc);
                                    insertStmt.setString(17, vwapPerc);
                                    insertStmt.setDouble(18, days120);
                                    insertStmt.setDouble(19, days180);
                                    insertStmt.setDouble(20, weeks52High);
                                    insertStmt.setDouble(21, weeks52Low);
                                    insertStmt.executeUpdate();
                                }
                            }
                        }
                    }
                } catch (NumberFormatException e) {
                    System.err.println("Error parsing data: " + e.getMessage());
                }
            }
        }
    }

    private static double parseDouble(String text) {
        try {
            return Double.parseDouble(text.replaceAll(",", ""));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static int parseInt(String text) {
        try {
            return Integer.parseInt(text.replaceAll(",", ""));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    static boolean tableExists(Connection conn, String tableName) {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeQuery("SELECT 1 FROM " + tableName + " LIMIT 1");
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    private static void storeLastUpdateOfTheDay() throws SQLException {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
            Map<String, Integer> updateCounts = new HashMap<>();
            String selectSql = "SELECT DISTINCT symbol, date FROM yearly_data";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(selectSql)) {
                while (rs.next()) {
                    String symbol = rs.getString("symbol");
                    LocalDate date = rs.getDate("date").toLocalDate();
                    String tableName = "daily_data_" + symbol.replaceAll("\\W", "_").toLowerCase();  // Normalize to lowercase

                    if (!tableExists(conn, tableName)) {
                        String createTableSql = "CREATE TABLE " + tableName + " (" +
                                "date DATE," +
                                "open DOUBLE," +
                                "high DOUBLE," +
                                "low DOUBLE," +
                                "close DOUBLE," +
                                "PRIMARY KEY (date)" +
                                ")";
                        try (Statement createStmt = conn.createStatement()) {
                            createStmt.executeUpdate(createTableSql);
                            System.out.println("\u001B[32m"+"Table created: " + tableName+"\u001B[0m");
                        }
                    }

                    String insertSql = "INSERT INTO " + tableName + " (date, open, high, low, close) " +
                            "SELECT date, open, high, low, close, FROM yearly_data WHERE symbol = ? AND date = ? " +
                            "ON DUPLICATE KEY UPDATE " +
                            "open = VALUES(open), high = VALUES(high), low = VALUES(low), close = VALUES(close)";
                    try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                        pstmt.setString(1, symbol);
                        pstmt.setObject(2, date);
                        int rowsAffected = pstmt.executeUpdate();
                        if (rowsAffected > 0) {
                            updateCounts.put(tableName, updateCounts.getOrDefault(tableName, 0) + 1);  // Increment count for tableName
                        }
                        System.out.println("Table updated: " + tableName + " Updated count: " + updateCounts.get(tableName));
                    }
                }
            }
            System.out.println("Total updates for each table:");
            for (Map.Entry<String, Integer> entry : updateCounts.entrySet()) {
                System.out.println("Table " + entry.getKey() + " updated " + entry.getValue() + " times");
            }
        }
    }



    private static Map<String, Object> getLastData(String symbol) throws SQLException {
        Map<String, Object> data = new HashMap<>();
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
            String tableName = "daily_data_" + symbol.replace("-", "_");
            String sql = "SELECT * FROM " + tableName + " WHERE date = (SELECT MAX(date) FROM " + tableName + ")";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    ResultSetMetaData rsmd = rs.getMetaData();
                    int columnCount = rsmd.getColumnCount();
                    for (int i = 1; i <= columnCount; i++) {
                        String name = rsmd.getColumnName(i);
                        Object value = rs.getObject(i);
                        data.put(name, value);
                    }
                }
            }
        }
        return data;
    }
}
