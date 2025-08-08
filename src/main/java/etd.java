import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class etd {
    private static final String JDBC_URL = "jdbc:mysql://localhost:3306/porfoliomanagment_nepse?createDatabaseIfNotExist=true";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "";
    private static final String FOLDER_PATH = "/Users/Mac//Desktop/mandalasystem/untitled/data/excel/1st";
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);

    public static void main(String[] args) {
        System.out.println("Program started.");
        try (Connection connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD)) {
            System.out.println("Connected to database.");
            createTableIfNotExists(connection);

            List<StockData> stockDataList = new ArrayList<>();
            Files.list(Path.of(FOLDER_PATH))
                    .filter(path -> path.getFileName().toString().toLowerCase().endsWith(".xlsx"))
                    .forEach(path -> {
                        System.out.println("Processing file: " + path.getFileName());
                        processExcelFile(path, stockDataList);
                    });

            deleteExistingData(connection);

            insertStockData(connection, stockDataList);
            System.out.println("Data insertion complete.");
        } catch (IOException | SQLException e) {
            System.err.println("Error during processing: " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println("Program ended.");
    }

    private static void processExcelFile(Path path, List<StockData> stockDataList) {
        try (Workbook workbook = new XSSFWorkbook(new FileInputStream(path.toFile()))) {
            Sheet sheet = workbook.getSheetAt(0);
            Iterator<Row> rowIterator = sheet.iterator();
            rowIterator.next(); // Skip header row

            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                try {
                    StockData stockData = parseStockData(row);
                    stockDataList.add(stockData);  // Remove the if condition
                } catch (DateTimeParseException | NumberFormatException e) {
                    System.err.println("Error parsing row: " + row.getRowNum() + " - " + e.getMessage());
                }
            }
            System.out.println("File processed successfully: " + path.getFileName());
        } catch (IOException e) {
            System.err.println("Error reading Excel file: " + path.getFileName());
            e.printStackTrace();
        }
    }

    static class StockData {
        LocalDate date;
        String symbol;
        double open;
        double high;
        double low;
        double close;

        public StockData(LocalDate date, String symbol, double open, double high, double low, double close) {
            this.date = date;
            this.symbol = symbol;
            this.open = open;
            this.high = high;
            this.low = low;
            this.close = close;
        }
    }

    private static void createTableIfNotExists(Connection connection) throws SQLException {
        System.out.println("Creating table if it doesn't exist.");
        String createTableSQL = "CREATE TABLE IF NOT EXISTS yearly_data (" +
                "id INT AUTO_INCREMENT PRIMARY KEY," +
                "date DATE," +
                "symbol VARCHAR(50)," +
                "open DECIMAL(10, 2)," +
                "high DECIMAL(10, 2)," +
                "low DECIMAL(10, 2)," +
                "close DECIMAL(10, 2)" +
                ")";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSQL);
            System.out.println("Table creation checked.");
        }
    }

    private static StockData parseStockData(Row row) throws DateTimeParseException, NumberFormatException {
        String dateStr = getCellValue(row.getCell(22));
        LocalDate date;

        try {
            date = LocalDate.parse(dateStr, DATE_FORMATTER);
        } catch (Exception e) {
            dateStr = getCellValue(row.getCell(24));
            date = LocalDate.parse(dateStr, DATE_FORMATTER);
        }

        String symbol = getCellValue(row.getCell(1));
        double open = Double.parseDouble(getCellValue(row.getCell(3)));
        double high = Double.parseDouble(getCellValue(row.getCell(4)));
        double low = Double.parseDouble(getCellValue(row.getCell(5)));
        double close = Double.parseDouble(getCellValue(row.getCell(6)));
        return new StockData(date, symbol, open, high, low, close);
    }

    private static String getCellValue(Cell cell) {
        if (cell == null) {
            return "";
        }
        switch (cell.getCellType()) {
            case STRING:
                return cell.getStringCellValue();
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    return cell.getDateCellValue().toString();
                } else {
                    return String.valueOf(cell.getNumericCellValue());
                }
            case BOOLEAN:
                return String.valueOf(cell.getBooleanCellValue());
            case FORMULA:
                return cell.getCellFormula();
            default:
                return "";
        }
    }

    private static void deleteExistingData(Connection connection) throws SQLException {
        System.out.println("Deleting existing data.");
        String deleteSQL = "DELETE FROM yearly_data";
        try (Statement statement = connection.createStatement()) {
            int rowsDeleted = statement.executeUpdate(deleteSQL);
            System.out.println("Rows deleted: " + rowsDeleted);
        }
    }

    private static void insertStockData(Connection connection, List<StockData> stockDataList) throws SQLException {
        String sql = "INSERT INTO yearly_data (date, symbol, open, high, low, close) VALUES (?, ?, ?, ?, ?, ?)";

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            System.out.println("Inserting data.");
            for (StockData stockData : stockDataList) {
                statement.setDate(1, Date.valueOf(stockData.date));
                statement.setString(2, stockData.symbol);
                statement.setDouble(3, stockData.open);
                statement.setDouble(4, stockData.high);
                statement.setDouble(5, stockData.low);
                statement.setDouble(6, stockData.close);
                statement.addBatch();
            }

            int[] batchResults = statement.executeBatch();
            System.out.println("Batch insertion complete. Rows inserted: " + batchResults.length);
        }
    }
}