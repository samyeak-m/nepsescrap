import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class dailyDemo {

    private static final String DB_URL = "jdbc:mysql://localhost:3306/nepse_test?" +
            "createDatabaseIfNotExist=true&" +
            "rewriteBatchedStatements=true&" +  // Enable batch optimization
            "useServerPrepStmts=false&" +       // Disable server-side prepared statements for batch
            "allowMultiQueries=true&" +         // Allow multiple queries
            "autoReconnect=true&" +
            "useCompression=true";              // Enable compression
    private static final String DB_USER = "root";
    private static final String DB_PASS = "";
    private static final long INTERVAL = 60000;
    private static final LocalTime START_OF_DAY = LocalTime.of(13, 00);
    private static final LocalTime END_OF_DAY = LocalTime.of(13, 01);

    private static String lastHash = "";
    private static LocalDate lastEODRunDate = null;

    private static HikariDataSource dataSource;
    private static final ExecutorService EXEC = Executors.newFixedThreadPool(
            Math.min(8, Runtime.getRuntime().availableProcessors() * 2)); // match Hikari max

    static {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(DB_URL);
        config.setUsername(DB_USER);
        config.setPassword(DB_PASS);
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(30000);
        config.setIdleTimeout(600000);
        config.setMaxLifetime(1800000);
        dataSource = new HikariDataSource(config);
    }

    public static void main(String[] args) {
        try {
            createTableIfNotExists();
            System.out.println("Table checked/created successfully.");
        } catch (SQLException e) {
            System.err.println("Error creating table: " + e.getMessage());
            return;
        }

        while (true) {
            try {
                LocalTime now = LocalTime.now();
                LocalDate today = LocalDate.now();
                DayOfWeek dayOfWeek = today.getDayOfWeek();

                if (now.isBefore(START_OF_DAY) || now.isAfter(END_OF_DAY)) {
                    if (now.isAfter(END_OF_DAY) && (lastEODRunDate == null || !lastEODRunDate.equals(today))) {
                        storeLastUpdateOfTheDay();
                        lastEODRunDate = today;
                        System.out.println("Last update of the day recorded at " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                        lastHash = "";
                    }
                    System.out.println("Market is closed. Sleeping until next check.");
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
        // Use LocalDateTime so we can roll to tomorrow correctly
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime nextStart = now.toLocalDate().atTime(START_OF_DAY);
        if (!now.toLocalTime().isBefore(START_OF_DAY)) {
            nextStart = nextStart.plusDays(1);
        }
        return java.time.Duration.between(now, nextStart).toMillis();
    }

    private static long getSleepDurationUntilSunday() {
        LocalDateTime now = LocalDateTime.now();
        LocalDate nextSunday = now.toLocalDate().plusDays((7 - now.getDayOfWeek().getValue()) % 7);
        if (nextSunday.equals(now.toLocalDate())) {
            nextSunday = nextSunday.plusDays(7);
        }
        LocalDateTime nextStart = nextSunday.atTime(START_OF_DAY);
        return java.time.Duration.between(now, nextStart).toMillis();
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
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {

            String dailyTableSQL = "CREATE TABLE IF NOT EXISTS daily_data (" +
                    "id INT AUTO_INCREMENT PRIMARY KEY," +
                    "close DOUBLE(10,2)," +
                    "conf DOUBLE(10,2)," +
                    "date DATE," +
                    "diff VARCHAR(20)," +
                    "`range` DOUBLE(10,2)," +
                    "days_120 DOUBLE(10,2)," +
                    "days_180 DOUBLE(10,2)," +
                    "diff_perc VARCHAR(20)," +
                    "high DOUBLE(10,2)," +
                    "low DOUBLE(10,2)," +
                    "open DOUBLE(10,2)," +
                    "prev_close DOUBLE(10,2)," +
                    "range_perc DOUBLE(10,2)," +
                    "symbol VARCHAR(255)," +
                    "trans INT," +
                    "turnover DOUBLE(20,2)," +
                    "vwap VARCHAR(20)," +
                    "vwap_perc VARCHAR(20)," +
                    "vol DOUBLE(20,2)," +
                    "weeks_52_high DOUBLE(10,2)," +
                    "weeks_52_low DOUBLE(10,2)," +
                    "UNIQUE KEY unique_date_symbol (date, symbol)" +
                    ")";
            stmt.executeUpdate(dailyTableSQL);

            // Add helpful indexes (ignore if they already exist)
            try { stmt.executeUpdate("CREATE INDEX IF NOT EXISTS idx_daily_symbol ON daily_data(symbol)"); } catch (SQLException ignore) {}
            try { stmt.executeUpdate("CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_data(date)"); } catch (SQLException ignore) {}
        }
    }

    private static void scrapeAndStoreData(String content) throws SQLException {
        // No outer connection; each worker chunk gets its own connection and transaction
        Document doc = Jsoup.parse(content);
        // Narrow to tbody rows to skip header rows quickly
        Elements allRows = doc.select("table.table tbody tr");
        List<Element> rowList = new ArrayList<>(allRows);

        int chunkSize = 100; // larger chunks reduce overhead
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int i = 0; i < rowList.size(); i += chunkSize) {
            int start = i;
            int end = Math.min(i + chunkSize, rowList.size());
            List<Element> chunk = rowList.subList(start, end);

            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try (Connection connChunk = dataSource.getConnection()) {
                    connChunk.setAutoCommit(false);
                    processRowChunk(connChunk, chunk);
                    connChunk.commit();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }, EXEC);
            futures.add(future);
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        System.out.println("Batch processing complete. Processed " + rowList.size() + " rows.");
    }

    private static LocalDate getLastInsertedDate(Connection conn, String symbol) throws SQLException {
        String sql = "SELECT MAX(date) FROM daily_data WHERE symbol = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, symbol);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    java.sql.Date d = rs.getDate(1);
                    return d == null ? LocalDate.of(1970, 1, 1) : d.toLocalDate();
                }
            }
        }
        return LocalDate.of(1970, 1, 1);
    }

    private static void processRowChunk(Connection conn, List<Element> chunk) throws SQLException {
        // Prepare batch upsert once per chunk
        String insertSql = "INSERT INTO daily_data (symbol, date, conf, open, high, low, close, vwap, vol, prev_close, turnover, trans, diff, `range`, " +
                "diff_perc, range_perc, vwap_perc, days_120, days_180, weeks_52_high, weeks_52_low) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE " +
                "conf=VALUES(conf), open=VALUES(open), high=VALUES(high), low=VALUES(low), close=VALUES(close), " +
                "vwap=VALUES(vwap), vol=VALUES(vol), prev_close=VALUES(prev_close), turnover=VALUES(turnover), " +
                "trans=VALUES(trans), diff=VALUES(diff), `range`=VALUES(`range`), diff_perc=VALUES(diff_perc), " +
                "range_perc=VALUES(range_perc), vwap_perc=VALUES(vwap_perc), days_120=VALUES(days_120), " +
                "days_180=VALUES(days_180), weeks_52_high=VALUES(weeks_52_high), weeks_52_low=VALUES(weeks_52_low)";

        try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
            LocalDate date = LocalDate.now();
            int batchCount = 0;

            for (Element row : chunk) {
                Elements cells = row.select("td");
                if (cells.size() < 2) continue;

                String symbol = cellText(cells, 1).trim();
                if (symbol.isEmpty()) continue;

                // Parse values without mutating DOM
                double conf = parseDouble(cellText(cells, 2));
                double open = parseDouble(cellText(cells, 3));
                double high = parseDouble(cellText(cells, 4));
                double low = parseDouble(cellText(cells, 5));
                double close = parseDouble(cellText(cells, 6));
                String vwap = cellText(cells, 7);
                double vol = parseDouble(cellText(cells, 8));
                double prevClose = parseDouble(cellText(cells, 9));
                double turnover = parseDouble(cellText(cells,10));
                int trans = parseInt(cellText(cells,11));
                String diff = cellText(cells,12);
                double range = parseDouble(cellText(cells,13));
                String diffPerc = cellText(cells,14);
                double rangePerc = parseDouble(cellText(cells,15));
                String vwapPerc = cellText(cells,16);
                double days120 = parseDouble(cellText(cells,17));
                double days180 = parseDouble(cellText(cells,18));
                double weeks52High = parseDouble(cellText(cells,19));
                double weeks52Low = parseDouble(cellText(cells,20));

                // Bind
                pstmt.setString(1, symbol);
                pstmt.setObject(2, date);
                pstmt.setDouble(3, conf);
                pstmt.setDouble(4, open);
                pstmt.setDouble(5, high);
                pstmt.setDouble(6, low);
                pstmt.setDouble(7, close);
                pstmt.setString(8, vwap);
                pstmt.setDouble(9, vol);
                pstmt.setDouble(10, prevClose);
                pstmt.setDouble(11, turnover);
                pstmt.setInt(12, trans);
                pstmt.setString(13, diff);
                pstmt.setDouble(14, range);
                pstmt.setString(15, diffPerc);
                pstmt.setDouble(16, rangePerc);
                pstmt.setString(17, vwapPerc);
                pstmt.setDouble(18, days120);
                pstmt.setDouble(19, days180);
                pstmt.setDouble(20, weeks52High);
                pstmt.setDouble(21, weeks52Low);

                pstmt.addBatch();
                batchCount++;

                if (batchCount % 200 == 0) {
                    pstmt.executeBatch();
                    pstmt.clearBatch();
                }
            }
            if (batchCount % 200 != 0) {
                pstmt.executeBatch();
            }
        }
    }

    // Fast safe cell access (avoids DOM growth)
    private static String cellText(Elements cells, int idx) {
        return (idx >= 0 && idx < cells.size()) ? cells.get(idx).text() : "0";
    }

    private static void createAndInsertStockTable(Connection conn, String symbol, LocalDate date, double open, double high, double low, double close) throws SQLException {
        String tableName = "daily_data_" + symbol.replaceAll("\\W", "_").toLowerCase();

        // Create the table if it does not exist
        String createTableSQL = "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                "date DATE," +
                "open DOUBLE(10,2)," +
                "high DOUBLE(10,2)," +
                "low DOUBLE(10,2)," +
                "close DOUBLE(10,2)," +
                "UNIQUE KEY unique_date (date)" +
                ")";
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(createTableSQL);
        }

        // Insert or update data in the table
        String insertSql = "INSERT INTO " + tableName + " (date, open, high, low, close) " +
                "VALUES (?, ?, ?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE " +
                "open = VALUES(open), high = VALUES(high), low = VALUES(low), close = VALUES(close)";
        try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
            pstmt.setObject(1, date);
            pstmt.setDouble(2, open);
            pstmt.setDouble(3, high);
            pstmt.setDouble(4, low);
            pstmt.setDouble(5, close);
            pstmt.executeUpdate();
        }
    }

    private static void createAndInsertStockTablesBatch(Connection conn, Map<String, List<StockRecord>> stockData) throws SQLException {
        for (Map.Entry<String, List<StockRecord>> entry : stockData.entrySet()) {
            String symbol = entry.getKey();
            List<StockRecord> records = entry.getValue();
            String tableName = "daily_data_" + symbol.replaceAll("\\W", "_").toLowerCase();
            
            // Create table once
            String createTableSQL = "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                    "date DATE," +
                    "open DOUBLE(10,2)," +
                    "high DOUBLE(10,2)," +
                    "low DOUBLE(10,2)," +
                    "close DOUBLE(10,2)," +
                    "UNIQUE KEY unique_date (date)" +
                    ")";
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createTableSQL);
            }
            
            // Batch insert all records for this symbol
            String insertSql = "INSERT INTO " + tableName + " (date, open, high, low, close) " +
                    "VALUES (?, ?, ?, ?, ?) " +
                    "ON DUPLICATE KEY UPDATE " +
                    "open = VALUES(open), high = VALUES(high), low = VALUES(low), close = VALUES(close)";
            
            try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                for (StockRecord record : records) {
                    pstmt.setObject(1, record.date);
                    pstmt.setDouble(2, record.open);
                    pstmt.setDouble(3, record.high);
                    pstmt.setDouble(4, record.low);
                    pstmt.setDouble(5, record.close);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
        }
    }

    // Helper class for batch processing
    static class StockRecord {
        LocalDate date;
        double open, high, low, close;
        
        public StockRecord(LocalDate date, double open, double high, double low, double close) {
            this.date = date;
            this.open = open;
            this.high = high;
            this.low = low;
            this.close = close;
        }
    }

    private static void updateData(Connection conn, String symbol, LocalDate date, double conf, double open, double high, double low, double close,
                                   String vwap, double vol, double prevClose, double turnover, int trans, String diff, double range,
                                   String diffPerc, double rangePerc, String vwapPerc, double days120, double days180, double weeks52High, double weeks52Low) throws SQLException {
        String updateSql = "UPDATE daily_data SET conf = ?, open = ?, high = ?, low = ?, close = ?, vwap = ?, vol = ?, prev_close = ?, turnover = ?, " +
                "trans = ?, diff = ?, `range` = ?, diff_perc = ?, range_perc = ?, vwap_perc = ?, days_120 = ?, days_180 = ?, weeks_52_high = ?, weeks_52_low = ? " +
                "WHERE symbol = ? AND date = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(updateSql)) {
            pstmt.setDouble(1, conf);
            pstmt.setDouble(2, open);
            pstmt.setDouble(3, high);
            pstmt.setDouble(4, low);
            pstmt.setDouble(5, close);
            pstmt.setString(6, vwap);
            pstmt.setDouble(7, vol);
            pstmt.setDouble(8, prevClose);
            pstmt.setDouble(9, turnover);
            pstmt.setInt(10, trans);
            pstmt.setString(11, diff);
            pstmt.setDouble(12, range);
            pstmt.setString(13, diffPerc);
            pstmt.setDouble(14, rangePerc);
            pstmt.setString(15, vwapPerc);
            pstmt.setDouble(16, days120);
            pstmt.setDouble(17, days180);
            pstmt.setDouble(18, weeks52High);
            pstmt.setDouble(19, weeks52Low);
            pstmt.setString(20, symbol);
            pstmt.setObject(21, date);
            pstmt.executeUpdate();
        }
    }

    private static void insertData(Connection conn, String symbol, LocalDate date, double conf, double open, double high, double low, double close,
                                   String vwap, double vol, double prevClose, double turnover, int trans, String diff, double range,
                                   String diffPerc, double rangePerc, String vwapPerc, double days120, double days180, double weeks52High, double weeks52Low) throws SQLException {
        String insertSql = "INSERT INTO daily_data (symbol, date, conf, open, high, low, close, vwap, vol, prev_close, turnover, trans, diff, `range`, " +
                "diff_perc, range_perc, vwap_perc, days_120, days_180, weeks_52_high, weeks_52_low) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
            pstmt.setString(1, symbol);
            pstmt.setObject(2, date);
            pstmt.setDouble(3, conf);
            pstmt.setDouble(4, open);
            pstmt.setDouble(5, high);
            pstmt.setDouble(6, low);
            pstmt.setDouble(7, close);
            pstmt.setString(8, vwap);
            pstmt.setDouble(9, vol);
            pstmt.setDouble(10, prevClose);
            pstmt.setDouble(11, turnover);
            pstmt.setInt(12, trans);
            pstmt.setString(13, diff);
            pstmt.setDouble(14, range);
            pstmt.setString(15, diffPerc);
            pstmt.setDouble(16, rangePerc);
            pstmt.setString(17, vwapPerc);
            pstmt.setDouble(18, days120);
            pstmt.setDouble(19, days180);
            pstmt.setDouble(20, weeks52High);
            pstmt.setDouble(21, weeks52Low);
            pstmt.executeUpdate();
        }
    }

    private static double parseDouble(String str) {
        try {
            return Double.parseDouble(str.replace(",", ""));
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    private static int parseInt(String str) {
        try {
            return Integer.parseInt(str.replace(",", ""));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static void storeLastUpdateOfTheDay() {
        long start = System.currentTimeMillis();

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try (Statement st = conn.createStatement()) {
                try { st.executeUpdate("CREATE INDEX IF NOT EXISTS idx_yearly_symbol_date ON yearly_data(symbol, date)"); } catch (SQLException ignore) {}
            }

            // Get all symbols once
            List<String> symbols = new ArrayList<>();
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT DISTINCT symbol FROM yearly_data WHERE symbol IS NOT NULL AND symbol <> ''")) {
                while (rs.next()) symbols.add(rs.getString(1));
            }

            int totalSymbols = symbols.size();
            int processed = 0;
            long insertedTotal = 0;

            System.out.println("EOD insert-by-symbol started. Symbols: " + totalSymbols);

            for (String symbol : symbols) {
                String table = "daily_data_" + symbol.replaceAll("\\W", "_").toLowerCase();

                // Ensure per-symbol table exists
                try (Statement st = conn.createStatement()) {
                    st.executeUpdate("CREATE TABLE IF NOT EXISTS " + table + " (" +
                            "date DATE PRIMARY KEY," +
                            "open DOUBLE(10,2), high DOUBLE(10,2), low DOUBLE(10,2), close DOUBLE(10,2))");
                }

                // Collect stats before insert
                SymbolStats before = getSymbolStats(conn, symbol, table);

                int insertedNow = 0;

                if (before.missing > 0) {
                    // Fast path: insert only truly missing dates (fills gaps anywhere)
                    String insertMissing = "INSERT INTO " + table + " (date, open, high, low, close) " +
                            "SELECT y.date, y.open, y.high, y.low, y.close " +
                            "FROM yearly_data y " +
                            "LEFT JOIN " + table + " t ON t.date = y.date " +
                            "WHERE y.symbol = ? AND t.date IS NULL";
                    try (PreparedStatement ps = conn.prepareStatement(insertMissing)) {
                        ps.setString(1, symbol);
                        insertedNow += ps.executeUpdate();
                    }
                }

                // Recompute stats after missing-only insert
                SymbolStats mid = getSymbolStats(conn, symbol, table);

                // If still not complete (rare), resume strictly from last inserted date
                if (mid.missing > 0 && mid.lastDate != null) {
                    String resumeFromLast = "INSERT INTO " + table + " (date, open, high, low, close) " +
                            "SELECT y.date, y.open, y.high, y.low, y.close " +
                            "FROM yearly_data y " +
                            "WHERE y.symbol = ? AND y.date > ? " +
                            "ON DUPLICATE KEY UPDATE open=VALUES(open), high=VALUES(high), low=VALUES(low), close=VALUES(close)";
                    try (PreparedStatement ps = conn.prepareStatement(resumeFromLast)) {
                        ps.setString(1, symbol);
                        ps.setObject(2, mid.lastDate);
                        insertedNow += ps.executeUpdate();
                    }
                }

                // Final stats
                SymbolStats after = getSymbolStats(conn, symbol, table);
                insertedTotal += insertedNow;

                int left = Math.max(0, after.totalYearly - after.existing);
                System.out.printf(
                    "Symbol: %s | Inserted now: %d | Total: %d | Existing: %d | Left: %d | LastDate: %s -> %s%n",
                    symbol,
                    insertedNow,
                    after.totalYearly,
                    after.existing,
                    left,
                    before.lastDate == null ? "null" : before.lastDate.toString(),
                    after.lastDate == null ? "null" : after.lastDate.toString()
                );

                processed++;
                if (processed % 25 == 0 || processed == totalSymbols) {
                    long elapsed = System.currentTimeMillis() - start;
                    double perSym = processed / Math.max(1.0, (elapsed / 1000.0));
                    long etaMs = (long) ((totalSymbols - processed) / Math.max(0.1, perSym) * 1000);
                    System.out.printf("Progress: %d/%d symbols | Inserted rows: %d | Elapsed: %s | ETA: %s%n",
                            processed, totalSymbols, insertedTotal, formatDuration(elapsed), formatDuration(Math.max(0, etaMs)));
                }
            }

            conn.commit();
            long total = System.currentTimeMillis() - start;
            System.out.println("EOD insert-by-symbol completed. Symbols: " + totalSymbols +
                    ", rows inserted: " + insertedTotal + ", time: " + formatDuration(total));
        } catch (SQLException e) {
            System.err.println("Error during insert-by-symbol EOD: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    // Per-symbol stats helper
    private static SymbolStats getSymbolStats(Connection conn, String symbol, String table) throws SQLException {
        SymbolStats s = new SymbolStats();
        s.symbol = symbol;
        s.table = table;

        // Count distinct dates in yearly_data for this symbol (avoids overcount if duplicates exist)
        try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(DISTINCT date) FROM yearly_data WHERE symbol = ?")) {
            ps.setString(1, symbol);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) s.totalYearly = rs.getInt(1);
            }
        }

        // existing rows in per-symbol table
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + table)) {
            if (rs.next()) s.existing = rs.getInt(1);
        } catch (SQLException e) {
            // If table not found yet (race), treat as 0
            s.existing = 0;
        }

        // last inserted date in per-symbol table
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT MAX(date) FROM " + table)) {
            if (rs.next()) {
                java.sql.Date d = rs.getDate(1);
                s.lastDate = d == null ? null : d.toLocalDate();
            }
        } catch (SQLException e) {
            s.lastDate = null;
        }

        s.missing = Math.max(0, s.totalYearly - s.existing);
        return s;
    }

    // Holder for stats
    private static class SymbolStats {
        String symbol;
        String table;
        int totalYearly;
        int existing;
        int missing;
        LocalDate lastDate;
    }

    private static void createAndInsertYearlyDataTable(Connection conn) throws SQLException {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS yearly_data (" +
                "id INT AUTO_INCREMENT PRIMARY KEY," +
                "symbol VARCHAR(255) NOT NULL," +
                "date DATE NOT NULL," +
                "open DOUBLE(10,2) NOT NULL," +
                "high DOUBLE(10,2) NOT NULL," +
                "low DOUBLE(10,2) NOT NULL," +
                "close DOUBLE(10,2) NOT NULL," +
                "volume BIGINT NOT NULL," +
                "turnover DOUBLE(10,2) NOT NULL," +
                "PRIMARY KEY (symbol, date)" +
                ")";
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(createTableSQL);
        }
    }

    private static void insertIntoYearlyData(Connection conn, String symbol, LocalDate date, double open, double high, double low, double close, long volume, double turnover) throws SQLException {
        String insertSql = "INSERT INTO yearly_data (symbol, date, open, high, low, close, volume, turnover) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE " +
                "open = VALUES(open), high = VALUES(high), low = VALUES(low), close = VALUES(close), " +
                "volume = VALUES(volume), turnover = VALUES(turnover)";
        try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
            pstmt.setString(1, symbol);
            pstmt.setObject(2, date);
            pstmt.setDouble(3, open);
            pstmt.setDouble(4, high);
            pstmt.setDouble(5, low);
            pstmt.setDouble(6, close);
            pstmt.setLong(7, volume);
            pstmt.setDouble(8, turnover);
            pstmt.executeUpdate();
        }
    }

    private static void backfillFromYearlyStartingFromLast() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            List<String> symbols = new ArrayList<>();
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT DISTINCT symbol FROM yearly_data WHERE symbol IS NOT NULL AND symbol <> ''")) {
                while (rs.next()) symbols.add(rs.getString(1));
            }

            for (String symbol : symbols) {
                String table = "daily_data_" + symbol.replaceAll("\\W", "_").toLowerCase();

                try (Statement st = conn.createStatement()) {
                    st.executeUpdate("CREATE TABLE IF NOT EXISTS " + table + " (" +
                            "date DATE PRIMARY KEY," +
                            "open DOUBLE(10,2), high DOUBLE(10,2), low DOUBLE(10,2), close DOUBLE(10,2))");
                }

                String insert = "INSERT INTO " + table + " (date, open, high, low, close) " +
                        "SELECT y.date, y.open, y.high, y.low, y.close " +
                        "FROM yearly_data y " +
                        "LEFT JOIN " + table + " t ON t.date = y.date " +
                        "WHERE y.symbol = ? AND t.date IS NULL";
                try (PreparedStatement ps = conn.prepareStatement(insert)) {
                    ps.setString(1, symbol);
                    ps.executeUpdate();
                }
            }

            conn.commit();
        }
    }

    private static String formatDuration(long milliseconds) {
        if (milliseconds < 0) milliseconds = 0;
        long seconds = milliseconds / 1000;
        long hours = seconds / 3600;
        long minutes = (seconds % 3600) / 60;
        long secs = seconds % 60;
        return String.format("%02d:%02d:%02d", hours, minutes, secs);
    }
}