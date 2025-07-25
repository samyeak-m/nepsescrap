import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

public class ExcelFileTable {
    private static final String JDBC_URL = "jdbc:mysql://localhost:3306/nepse_new";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "";
    private static final String BASE_FOLDER_PATH = "D:/downloads/excel/";
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);

    public static void main(String[] args) {
        try (Connection connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD)) {
            int folderCounter = 1;

            for (int i = 10; i <= 13; i++) {
                String folderPath = BASE_FOLDER_PATH + i;
                System.out.println("Processing folder: " + folderPath); // Show the folder being processed

                int[] tableCounter = {1}; // Reset the table counter for each folder

                try {
                    Files.list(Path.of(folderPath))
                            .filter(path -> path.getFileName().toString().toLowerCase().endsWith(".xlsx"))
                            .forEach(path -> {
                                String tableName = getTableNameFromFilePath(path).toLowerCase(); // Ensure the table name is in lowercase
                                try {
                                    createTableIfNotExists(connection, tableName);
                                    List<StockData> stockDataList = processExcelFile(path);
                                    insertOrUpdateStockData(connection, tableName, stockDataList, tableCounter[0]);
                                    tableCounter[0]++;
                                } catch (SQLException e) {
                                    e.printStackTrace();
                                }
                            });
                } catch (IOException e) {
                    System.err.println("Error processing folder: " + folderPath);
                    e.printStackTrace();
                }

                folderCounter++;
            }
            System.out.println("Data insertion complete.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static String getTableNameFromFilePath(Path path) {
        String fileName = path.getFileName().toString();
        return fileName.substring(0, fileName.lastIndexOf('.')).replaceAll("[^a-zA-Z0-9_]", "");
    }

    private static List<StockData> processExcelFile(Path path) {
        List<StockData> stockDataList = new ArrayList<>();
        try (Workbook workbook = new XSSFWorkbook(new FileInputStream(path.toFile()))) {
            Sheet sheet = workbook.getSheetAt(0);
            Iterator<Row> rowIterator = sheet.iterator();
            rowIterator.next(); // Skip the header row

            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                try {
                    stockDataList.add(parseStockData(row));
                } catch (DateTimeParseException | NumberFormatException e) {
                    System.err.println("Error parsing row: " + row.getRowNum() + " - " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading Excel file: " + path.getFileName());
            e.printStackTrace();
        }
        return stockDataList;
    }

    static class StockData {
        LocalDate date;
        double open;
        double high;
        double low;
        double close;
        double volume;
        double turnover;

        public StockData(LocalDate date, double open, double high, double low, double close, double volume, double turnover) {
            this.date = date;
            this.open = open;
            this.high = high;
            this.low = low;
            this.close = close;
            this.volume = volume;
            this.turnover = turnover;
        }
    }

    private static void createTableIfNotExists(Connection connection, String tableName) throws SQLException {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                "date DATE PRIMARY KEY," +
                "open DECIMAL(10, 2)," +
                "high DECIMAL(10, 2)," +
                "low DECIMAL(10, 2)," +
                "close DECIMAL(10, 2)," +
                "volume DECIMAL(20, 2)," +
                "turnover DECIMAL(20, 2)" +
                ")";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSQL);
        }
    }

    private static StockData parseStockData(Row row) throws DateTimeParseException, NumberFormatException {
        String dateStr = getCellValue(row.getCell(19));
        LocalDate date = LocalDate.parse(dateStr, DATE_FORMATTER);
        double open = Double.parseDouble(getCellValue(row.getCell(3)));
        double high = Double.parseDouble(getCellValue(row.getCell(4)));
        double low = Double.parseDouble(getCellValue(row.getCell(5)));
        double close = Double.parseDouble(getCellValue(row.getCell(6)));
        double volume = Double.parseDouble(getCellValue(row.getCell(8)));
        double turnover = Double.parseDouble(getCellValue(row.getCell(10)));
        return new StockData(date, open, high, low, close, volume, turnover);
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

    private static void insertOrUpdateStockData(Connection connection, String tableName, List<StockData> stockDataList, int tableCounter) throws SQLException {
        String insertSQL = "INSERT INTO " + tableName + " (date, open, high, low, close, volume, turnover) VALUES (?, ?, ?, ?, ?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE open = VALUES(open), high = VALUES(high), low = VALUES(low), close = VALUES(close), volume = VALUES(volume), turnover = VALUES(turnover)";

        int[] updateCounts;
        try (PreparedStatement statement = connection.prepareStatement(insertSQL)) {
            for (StockData stockData : stockDataList) {
                statement.setDate(1, Date.valueOf(stockData.date));
                statement.setDouble(2, stockData.open);
                statement.setDouble(3, stockData.high);
                statement.setDouble(4, stockData.low);
                statement.setDouble(5, stockData.close);
                statement.setDouble(6, stockData.volume);
                statement.setDouble(7, stockData.turnover);
                statement.addBatch();
            }
            updateCounts = statement.executeBatch();
        }

        // Printing out table name with a counter
        System.out.println(tableCounter + " table: " + tableName);

        // Counting the number of rows updated
        int totalUpdatedRows = 0;
        for (int count : updateCounts) {
            if (count == Statement.EXECUTE_FAILED) {
                totalUpdatedRows++;
            } else if (count > 0) {
                totalUpdatedRows += count;
            }
        }

        // Printing out the number of rows updated
        System.out.println("Total rows updated: " + totalUpdatedRows);
    }
}
