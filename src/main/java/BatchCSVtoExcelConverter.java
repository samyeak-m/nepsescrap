import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import java.io.*;
import java.nio.file.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class BatchCSVtoExcelConverter {

    public static void convertCSVsToExcel(String inputFolderPath, String outputFolderPath) throws IOException {
        File inputFolder = new File(inputFolderPath);
        File outputFolder = new File(outputFolderPath);

        // Create output folder if it doesn't exist
        if (!outputFolder.exists()) {
            outputFolder.mkdirs();
        }

        // Loop through each file in the input folder
        for (File csvFile : inputFolder.listFiles()) {
            if (csvFile.isFile() && csvFile.getName().toLowerCase().endsWith(".csv")) {
                String excelFileName = csvFile.getName().replace(".csv", ".xlsx");
                String csvFilePath = csvFile.getAbsolutePath();
                String excelFilePath = Paths.get(outputFolderPath, excelFileName).toString();
                convertCSVtoExcel(csvFilePath, excelFilePath);
                System.out.println("Converted " + csvFile.getName() + " to " + excelFileName);
            }
        }
    }

    public static void convertCSVtoExcel(String csvFilePath, String excelFilePath) throws IOException {
        Workbook workbook = new XSSFWorkbook();
        Sheet sheet = workbook.createSheet("Data");

        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
            String line;
            int rowNum = 0;
            while ((line = br.readLine()) != null) {
                String[] data = line.split(",");
                Row row = sheet.createRow(rowNum++);
                int colNum = 0;
                for (String value : data) {
                    Cell cell = row.createCell(colNum++);

                    // Enhanced Data Type Detection
                    if (isNumeric(value)) {
                        cell.setCellValue(Double.parseDouble(value));
                        cell.setCellType(CellType.NUMERIC);
                    } else if (isDate(value)) {
                        try {
                            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd"); // Adjust date format if needed
                            Date date = dateFormat.parse(value);
                            cell.setCellValue(date);
                            cell.setCellType(CellType.NUMERIC);
                            CellStyle cellStyle = workbook.createCellStyle();
                            CreationHelper createHelper = workbook.getCreationHelper();
                            cellStyle.setDataFormat(createHelper.createDataFormat().getFormat("yyyy-MM-dd"));
                            cell.setCellStyle(cellStyle);
                        } catch (ParseException e) {
                            cell.setCellValue(value);
                            cell.setCellType(CellType.STRING);
                        }
                    } else {
                        cell.setCellValue(value);
                        cell.setCellType(CellType.STRING);
                    }

                    // Additional Formatting: Auto-size columns (optional)
                    sheet.autoSizeColumn(colNum - 1);
                }
            }
        }

        try (FileOutputStream outputStream = new FileOutputStream(excelFilePath)) {
            workbook.write(outputStream);
        }
    }

    private static boolean isNumeric(String str) {
        return str.matches("-?\\d+(\\.\\d+)?");
    }

    private static boolean isDate(String str) {
        // You can customize this date pattern as needed
        return str.matches("\\d{4}-\\d{2}-\\d{2}");
    }

    public static void main(String[] args) {
        String inputFolderPath = "D:/downloads/csv/09";
        String outputFolderPath = "D:/downloads/excel/09";

        try {
            convertCSVsToExcel(inputFolderPath, outputFolderPath);
            System.out.println("Conversion completed successfully.");
        } catch (IOException e) {
            System.err.println("Error occurred while converting CSVs to Excel: " + e.getMessage());
        }
    }
}
