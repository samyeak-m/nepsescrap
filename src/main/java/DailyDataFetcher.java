import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Base64;

public class DailyDataFetcher {

    // Database connection details
    private static final String DB_URL = "jdbc:mysql://localhost:3306/nepse_livedata";
    private static final String DB_USER = "root";
    private static final String DB_PASS = "";

    // Scraping and update settings
    private static final long UPDATE_INTERVAL_MS = 60000; // 1 minute
    private static final LocalTime MARKET_OPEN = LocalTime.of(10, 45);
    private static final LocalTime MARKET_CLOSE = LocalTime.of(15, 15);

    // Data tracking
    private static String lastHash = "";

    // Selenium WebDriver setup (adjust path if needed)
    private static final String CHROME_DRIVER_PATH = "D:\\downloads\\chromedriver.exe"; // Replace with your path

    public static void main(String[] args) {
        try {
            createTableIfNotExists();
        } catch (SQLException e) {
            System.err.println("Error creating table: " + e.getMessage());
            return;
        }

        ChromeOptions options = new ChromeOptions();
        // Disabling the "DevToolsActivePort" check
        options.setExperimentalOption("excludeSwitches", new String[]{"enable-automation"});
        options.addArguments("--disable-blink-features=AutomationControlled");

        System.setProperty("webdriver.chrome.driver", CHROME_DRIVER_PATH);
        WebDriver driver = new ChromeDriver(options);
        try {
            fetchAndStoreDataForDate(driver, "2023-12-12"); // or LocalDate.now().toString(); for current date
        } catch (SQLException | IOException | NoSuchAlgorithmException e) {
            System.err.println("Error fetching or storing data: " + e.getMessage());
        } finally {
            driver.quit();
        }
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

    private static void fetchAndStoreDataForDate(WebDriver driver, String targetDate) throws SQLException, IOException, NoSuchAlgorithmException {
        String url = "https://www.sharesansar.com/today-share-price";
        driver.get(url);

        // Locate date input field and button
        WebElement datePicker = new WebDriverWait(driver, 10)
                .until(ExpectedConditions.elementToBeClickable(By.className("form-control.datepicker")));
        WebElement searchButton = driver.findElement(By.className("btn.btn-org"));

        // Input the date and click the button
        datePicker.clear();
        datePicker.sendKeys(targetDate);
        searchButton.click();

        // Wait for the page to load with the selected date
        new WebDriverWait(driver, 10).until(ExpectedConditions.urlContains("date=" + targetDate));

        // Get page source after interaction
        String content = driver.getPageSource();
        String currentHash = generateHash(content);

        if (!currentHash.equals(lastHash)) {
            scrapeAndStoreData(content, targetDate);
            lastHash = currentHash;
            System.out.println("Data updated for " + targetDate + " at " + LocalDateTime.now());
        } else {
            System.out.println("Data for " + targetDate + " remains the same. Skipping update.");
        }
    }

    private static void scrapeAndStoreData(String content, String targetDate) throws SQLException {
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
                    LocalDate date = LocalDate.parse(targetDate);
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

                    String checkSql = "SELECT COUNT(*) FROM daily_data WHERE symbol = ? AND date = ?";
                    try (PreparedStatement checkStmt = conn.prepareStatement(checkSql)) {
                        checkStmt.setString(1, symbol);
                        checkStmt.setObject(2, date);
                        try (ResultSet rs = checkStmt.executeQuery()) {
                            if (rs.next() && rs.getInt(1) > 0) {
                                String updateSql = "UPDATE daily_data SET conf = ?, open = ?, high = ?, low = ?, close = ?, vwap = ?, vol = ?, prev_close = ?, turnover = ?, trans = ?, diff = ?, `range` = ?, diff_perc = ?, range_perc = ?, vwap_perc = ?, days_120 = ?, days_180 = ?, weeks_52_high = ?, weeks_52_low = ? WHERE symbol = ? AND date = ?";
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
                            } else {
                                String insertSql = "INSERT INTO daily_data (date, symbol, conf, open, high, low, close, vwap, vol, prev_close, turnover, trans, diff, `range`, diff_perc, range_perc, vwap_perc, days_120, days_180, weeks_52_high, weeks_52_low) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                                try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                                    pstmt.setObject(1, date);
                                    pstmt.setString(2, symbol);
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
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Error processing row: " + e.getMessage());
                }
            }
        }
    }

    private static double parseDouble(String text) {
        try {
            return Double.parseDouble(text.replace(",", ""));
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    private static int parseInt(String text) {
        try {
            return Integer.parseInt(text.replace(",", ""));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static String generateHash(String input) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(hash);
    }
}
