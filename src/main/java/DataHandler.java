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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;

public class DataHandler {

    private static final String DB_URL = "jdbc:mysql://localhost:3306/nepse_demodata";
    private static final String DB_USER = "root";
    private static final String DB_PASS = "";
    private static final long INTERVAL = 60000;
    private static final LocalTime START_OF_DAY = LocalTime.of(7, 45);
    private static final LocalTime END_OF_DAY = LocalTime.of(15, 15);

    private static String lastHash = "";

    public static void main(String[] args) {
        try {
            createTablesIfNotExists();
            System.out.println("Tables checked/created successfully.");
        } catch (SQLException e) {
            System.err.println("Error creating tables: " + e.getMessage());
            return;
        }

        while (true) {
            try {
                LocalTime now = LocalTime.now();
                if (now.isBefore(START_OF_DAY) || now.isAfter(END_OF_DAY)) {
                    if (now.isAfter(END_OF_DAY)) {
                        fetchAndStoreDailyData();
                        System.out.println("Daily data updated successfully at " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                        lastHash = "";
                    }
                    System.out.println("Market is closed. Sleeping until next check.");
                    Thread.sleep(getSleepDuration());
                    continue;
                }

                String liveDataURL = "https://www.sharesansar.com/live-trading";
                String content = fetchData(liveDataURL);
                String currentHash = generateHash(content);

                if (!currentHash.equals(lastHash)) {
                    scrapeAndStoreLiveData(content);
                    lastHash = currentHash;
                    LocalDateTime nowDateTime = LocalDateTime.now();
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    String formattedNow = nowDateTime.format(formatter);
                    System.out.println("Live data updated successfully at " + formattedNow);
                } else {
                    System.out.println("Live data remains the same. Skipping database update.");
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
        return now.isAfter(nextStart) ? java.time.Duration.between(nextStart, now).toMillis() : java.time.Duration.between(now, nextStart).toMillis();
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

    private static void createTablesIfNotExists() throws SQLException {
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

    private static void scrapeAndStoreLiveData(String content) throws SQLException {
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
                    double vol = parseDouble(cells.get(8).text());
                    double prevClose = parseDouble(cells.get(9).text());

                    String checkSql = "SELECT COUNT(*) FROM daily_data WHERE symbol = ?";
                    try (PreparedStatement checkStmt = conn.prepareStatement(checkSql)) {
                        checkStmt.setString(1, symbol);
                        try (ResultSet rs = checkStmt.executeQuery()) {
                            if (rs.next() && rs.getInt(1) > 0) {
                                String updateSql = "UPDATE daily_data SET date = ?, ltp = ?, pointChange = ?, perChange = ?, open = ?, high = ?, low = ?, vol = ?, prev_close = ? WHERE symbol = ?";
                                try (PreparedStatement pstmt = conn.prepareStatement(updateSql)) {
                                    pstmt.setObject(1, date);
                                    pstmt.setDouble(2, ltp);
                                    pstmt.setString(3, pointChange);
                                    pstmt.setString(4, perChange);
                                    pstmt.setDouble(5, open);
                                    pstmt.setDouble(6, high);
                                    pstmt.setDouble(7, low);
                                    pstmt.setDouble(8, vol);
                                    pstmt.setDouble(9, prevClose);
                                    pstmt.setString(10, symbol);
                                    pstmt.executeUpdate();
                                }
                            } else {
                                String insertSql = "INSERT INTO daily_data (date, symbol, conf, open, high, low, close, vwap, vol, prev_close, turnover, trans, diff, `range`, diff_perc, range_perc, vwap_perc, days_120, days_180, weeks_52_high, weeks_52_low) " +
                                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                                try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                                    pstmt.setObject(1, date);
                                    pstmt.setString(2, symbol);
                                    pstmt.setDouble(3, parseDouble(cells.get(2).text()));
                                    pstmt.setDouble(4, parseDouble(cells.get(3).text()));
                                    pstmt.setDouble(5, parseDouble(cells.get(4).text()));
                                    pstmt.setDouble(6, parseDouble(cells.get(5).text()));
                                    pstmt.setDouble(7, parseDouble(cells.get(6).text()));
                                    pstmt.setString(8, cells.get(7).text());
                                    pstmt.setDouble(9, parseDouble(cells.get(8).text()));
                                    pstmt.setDouble(10, parseDouble(cells.get(9).text()));
                                    pstmt.setDouble(11, parseDouble(cells.get(10).text()));
                                    pstmt.setInt(12, parseInt(cells.get(11).text()));
                                    pstmt.setString(13, cells.get(12).text());
                                    pstmt.setDouble(14, parseDouble(cells.get(13).text()));
                                    pstmt.setString(15, cells.get(14).text());
                                    pstmt.setDouble(16, parseDouble(cells.get(15).text()));
                                    pstmt.setString(17, cells.get(16).text());
                                    pstmt.setDouble(18, parseDouble(cells.get(17).text()));
                                    pstmt.setDouble(19, parseDouble(cells.get(18).text()));
                                    pstmt.setDouble(20, parseDouble(cells.get(19).text()));
                                    pstmt.setDouble(21, parseDouble(cells.get(20).text()));
                                    pstmt.executeUpdate();
                                }
                            }
                        }
                    }
                } catch (NumberFormatException e) {
                    System.err.println("Skipping row due to number format error: " + e.getMessage() + " - Row data: " + row.text());
                } catch (SQLException e) {
                    System.err.println("SQL error while inserting row: " + e.getMessage() + " - Row data: " + row.text());
                }
            }
        }
    }

    private static void fetchAndStoreDailyData() throws SQLException {
        String dailyDataURL = "https://www.sharesansar.com/today-share-price";
        try {
            String content = fetchData(dailyDataURL);
            scrapeAndStoreLiveData(content);  // We use the same method since the structure is similar
        } catch (IOException e) {
            System.err.println("Error fetching daily data: " + e.getMessage());
        }
    }

    private static double parseDouble(String text) {
        try {
            return Double.parseDouble(text.replace(",", "").replace("-", "0"));
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    private static int parseInt(String text) {
        try {
            return Integer.parseInt(text.replace(",", "").replace("-", "0"));
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
