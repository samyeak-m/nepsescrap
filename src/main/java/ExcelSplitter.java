import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

import org.apache.poi.util.IOUtils;

public class ExcelSplitter {

    public static void main(String[] args) {
        // Set a higher override value for the maximum allowable size
        IOUtils.setByteArrayMaxOverride(2000_000_000); // Set to a value higher than the default

        // The rest of your code remains the same
        try {
            // Load the Excel file
            FileInputStream file = new FileInputStream("D:/downloads/excel/nepse_data.xlsx");
            Workbook workbook = new XSSFWorkbook(file);
            Sheet sheet = workbook.getSheetAt(0); // Assuming your data is on the first sheet

            // Initialize a map to store data for each date
            Map<Date, List<Row>> dataMap = new HashMap<>();

            // Iterate through each row and organize data by date
            for (Row row : sheet) {
                Date date = row.getCell(19).getDateCellValue(); // Assuming date is in the first column
                if (!dataMap.containsKey(date)) {
                    dataMap.put(date, new ArrayList<>());
                }
                dataMap.get(date).add(row);
            }

            // Export data to separate Excel files for each date
            for (Map.Entry<Date, List<Row>> entry : dataMap.entrySet()) {
                Date date = entry.getKey();
                List<Row> rows = entry.getValue();

                // Create a new workbook for each date
                Workbook dateWorkbook = new XSSFWorkbook();
                Sheet dateSheet = dateWorkbook.createSheet("Data");

                // Copy rows to the new workbook
                int rowNum = 0;
                for (Row row : rows) {
                    Row newRow = dateSheet.createRow(rowNum++);
                    for (int i = 0; i < row.getLastCellNum(); i++) {
                        Cell oldCell = row.getCell(i);
                        Cell newCell = newRow.createCell(i);
                        if (oldCell != null) {
                            newCell.setCellValue(oldCell.getStringCellValue());
                        }
                    }
                }

                // Write the new workbook to a file
                String fileName = "data_" + date.getTime() + ".xlsx"; // Use timestamp as file name
                FileOutputStream outputStream = new FileOutputStream(fileName);
                dateWorkbook.write(outputStream);
                dateWorkbook.close();
            }

            workbook.close();
            file.close();

            System.out.println("Excel files have been generated successfully!");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
