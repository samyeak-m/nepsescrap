import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import java.io.*;
import java.nio.file.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.ArrayList;
import java.util.List;

public class BatchCSVtoExcelConverter {

    public static void convertCSVsToExcel(String inputFolderPath, String outputFolderPath) throws IOException {
        File inputFolder = new File(inputFolderPath);
        File outputFolder = new File(outputFolderPath);

        if (!inputFolder.exists() || !inputFolder.isDirectory()) {
            System.err.println("Input folder does not exist or is not a directory: " + inputFolderPath);
            return;
        }

        if (!outputFolder.exists()) {
            outputFolder.mkdirs();
        }

        File[] files = inputFolder.listFiles();
        if (files == null) {
            System.err.println("No files found in input folder: " + inputFolderPath);
            return;
        }

        // Filter CSV files up front
        List<File> csvFiles = new ArrayList<>();
        for (File file : files) {
            if (file.isFile() && file.getName().toLowerCase().endsWith(".csv")) {
                csvFiles.add(file);
            }
        }

        int totalFiles = csvFiles.size();
        if (totalFiles == 0) {
            System.out.println("No CSV files found in input folder.");
            return;
        }

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < totalFiles; i++) {
            File csvFile = csvFiles.get(i);
            String excelFileName = csvFile.getName().replace(".csv", ".xlsx");
            String csvFilePath = csvFile.getAbsolutePath();
            String excelFilePath = Paths.get(outputFolderPath, excelFileName).toString();

            long fileStart = System.currentTimeMillis();
            convertCSVtoExcel(csvFilePath, excelFilePath);
            long fileEnd = System.currentTimeMillis();

            int filesDone = i + 1;
            int filesLeft = totalFiles - filesDone;
            long elapsed = fileEnd - startTime;
            double avgPerFile = (double) elapsed / filesDone;
            long estimatedTotal = (long) (avgPerFile * totalFiles);
            long remaining = estimatedTotal - elapsed;

            System.out.printf(
                "Converted %s (%d/%d). Time for this file: %.2fs | Files left: %d | ETA: %s\n",
                csvFile.getName(), filesDone, totalFiles, (fileEnd - fileStart) / 1000.0, filesLeft,
                formatTimeMillis(remaining)
            );
        }

        long endTime = System.currentTimeMillis();
        System.out.println("Conversion completed successfully.");
        System.out.println("Total time: " + formatTimeMillis(endTime - startTime));
    }

    // Utility method to format milliseconds as hh:mm:ss
    private static String formatTimeMillis(long millis) {
        long seconds = millis / 1000;
        long minutes = seconds / 60;
        long hours = minutes / 60;
        seconds %= 60;
        minutes %= 60;
        return String.format("%02d:%02d:%02d", hours, minutes, seconds);
    }

    public static void convertCSVtoExcel(String csvFilePath, String excelFilePath) throws IOException {
        Workbook workbook = new XSSFWorkbook();
        Sheet sheet = workbook.createSheet("Data");

        CellStyle dateCellStyle = workbook.createCellStyle();
        CreationHelper createHelper = workbook.getCreationHelper();
        dateCellStyle.setDataFormat(createHelper.createDataFormat().getFormat("yyyy-MM-dd"));
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd"); // outside loop

        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
            String line;
            int rowNum = 0;
            int numCols = 0;

            while ((line = br.readLine()) != null) {
                String[] data = line.split(",", -1); // keep empty cells
                Row row = sheet.createRow(rowNum++);
                numCols = Math.max(numCols, data.length); // track col count

                for (int colNum = 0; colNum < data.length; colNum++) {
                    String value = data[colNum];
                    Cell cell = row.createCell(colNum);
                    if (isNumeric(value)) {
                        cell.setCellValue(Double.parseDouble(value));
                        cell.setCellType(CellType.NUMERIC);
                    } else if (isDate(value)) {
                        try {
                            Date date = dateFormat.parse(value);
                            cell.setCellValue(date);
                            cell.setCellType(CellType.NUMERIC);
                            cell.setCellStyle(dateCellStyle);
                        } catch (ParseException e) {
                            cell.setCellValue(value);
                            cell.setCellType(CellType.STRING);
                        }
                    } else {
                        cell.setCellValue(value);
                        cell.setCellType(CellType.STRING);
                    }
                }
            }
            // Auto-size columns after all data is written
            for (int col = 0; col < numCols; col++) {
                sheet.autoSizeColumn(col);
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
        return str.matches("\\d{4}-\\d{2}-\\d{2}");
    }

    public static void main(String[] args) {
        String inputFolderPath = "/Users/Mac/Desktop/mandalasystem/untitled/data/csv/2025";
        String outputFolderPath = "/Users/Mac//Desktop/mandalasystem/untitled/data/excel/2025";

        try {
            convertCSVsToExcel(inputFolderPath, outputFolderPath);
            System.out.println("Conversion completed successfully.");
        } catch (IOException e) {
            System.err.println("Error occurred while converting CSVs to Excel: " + e.getMessage());
        }
    }
}