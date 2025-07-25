import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

public class MonteCarloVaR {

    private static final int NUM_SIMULATIONS = 10000;

    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);

        String green = "\u001B[32m";
        String yellow = "\u001B[33m";
        String blue = "\u001B[34m";
        String reset = "\u001B[0m";

        String url = "jdbc:mysql://localhost:3306/nepse_test";
        String user = "root";
        String password = "";

        createVarOfAllDataTable(url, user, password);

        if (isTableEmpty(url, user, password)) {
            List<String> stockSymbols = getAllStockSymbols(url, user, password);
            Random random = new Random();

            for (String stockSymbol : stockSymbols) {
                int randomDays = 10 + random.nextInt(250);
                double randomConfidenceLevel = 0.90 + (0.09 * random.nextDouble());
                calculateAndStoreVaR(url, user, password, stockSymbol, randomDays, randomConfidenceLevel, true);
            }
        }

        boolean continueCalculating = true;

        while (continueCalculating) {
            System.out.print("Enter the stock symbol: ");
            String stockSymbol = scanner.nextLine();

            System.out.print("Enter the number of days for the investment (more than 2 days): ");
            int daysOfInvestment = scanner.nextInt();
            scanner.nextLine();

            double confidenceLevel = calculateDynamicConfidenceLevel(url, user, password, stockSymbol);

            List<Double> closePrices = getClosePrices(url, user, password, stockSymbol);

            if (closePrices.isEmpty()) {
                System.out.println("No data available for the given stock symbol.");
                return;
            }

            if (closePrices.size() < daysOfInvestment) {
                daysOfInvestment = closePrices.size();
            }

            double initialStockPrice = closePrices.get(closePrices.size() - 1);
            System.out.printf(blue + "Initial Stock Price: Rs.%.2f%n" + yellow, initialStockPrice);

            double meanReturn = calculateMeanReturn(closePrices, daysOfInvestment);
            double volatility = calculateVolatility(closePrices, daysOfInvestment, meanReturn);

            double var = calculateVaR(initialStockPrice, meanReturn, volatility, daysOfInvestment, NUM_SIMULATIONS, confidenceLevel);
            double varPercentage = (var / initialStockPrice) * 100;
            System.out.printf(blue + "Value at Risk (VaR) at %.2f%% confidence level: Rs.%.2f (%.2f%% of initial investment)%n", confidenceLevel * 100, var, varPercentage);

            System.out.println(green + "Historical Close Prices: " + yellow + closePrices);
            System.out.printf(green + "Mean Return: %.6f%n" + yellow, meanReturn);
            System.out.printf(green + "Volatility: %.6f%n" + yellow, volatility);
            System.out.printf(green + "Initial Stock Price: %.6f%n" + yellow, initialStockPrice);

            storeVaRData(url, user, password, stockSymbol, daysOfInvestment, meanReturn, volatility, var, initialStockPrice, confidenceLevel);

            System.out.print(reset + "Do you want to calculate VaR for another stock? (yes/no): ");
            String response = scanner.nextLine();
            if (!response.equalsIgnoreCase("yes")) {
                continueCalculating = false;
            }
        }

        scanner.close();
    }

    public static void createVarOfAllDataTable(String url, String user, String password) throws Exception {
        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement()) {

            String createTableSQL = "CREATE TABLE IF NOT EXISTS varofall_data (" +
                    "id INT AUTO_INCREMENT PRIMARY KEY, " +
                    "stock_symbol VARCHAR(20), " +
                    "date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
                    "days_of_investment INT, " +
                    "mean_return DOUBLE, " +
                    "volatility DOUBLE, " +
                    "var DOUBLE, " +
                    "initial_stock_price DOUBLE," +
                    "confidence_level DOUBLE)";
            stmt.execute(createTableSQL);
        }
    }

    public static boolean isTableEmpty(String url, String user, String password) throws Exception {
        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement()) {

            String query = "SELECT COUNT(*) FROM varofall_data";
            ResultSet rs = stmt.executeQuery(query);
            if (rs.next()) {
                return rs.getInt(1) == 0;
            }
        }
        return true;
    }

    public static List<String> getAllStockSymbols(String url, String user, String password) throws Exception {
        List<String> stockSymbols = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement()) {

            String query = "SHOW TABLES LIKE 'daily_data_%'";
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                String tableName = rs.getString(1);
                String stockSymbol = tableName.replace("daily_data_", "");
                stockSymbols.add(stockSymbol);
            }
        }

        return stockSymbols;
    }

    public static void calculateAndStoreVaR(String url, String user, String password, String stockSymbol, int daysOfInvestment, double confidenceLevel, boolean forAll) throws Exception {
        List<Double> closePrices = getClosePrices(url, user, password, stockSymbol);

        if (closePrices.isEmpty()) {
            System.out.println("No data available for the given stock symbol: " + stockSymbol);
            return;
        }

        if (closePrices.size() < daysOfInvestment) {
            daysOfInvestment = closePrices.size();
        }

        double initialStockPrice = closePrices.get(closePrices.size() - 1);

        double meanReturn = calculateMeanReturn(closePrices, daysOfInvestment);
        double volatility = calculateVolatility(closePrices, daysOfInvestment, meanReturn);

        double var = calculateVaR(initialStockPrice, meanReturn, volatility, daysOfInvestment, NUM_SIMULATIONS, confidenceLevel);

        if (Double.isNaN(meanReturn) || Double.isNaN(volatility) || Double.isNaN(var) || Double.isNaN(initialStockPrice)) {
            System.out.printf("Skipping stock symbol %s due to NaN values in calculations.%n", stockSymbol);
            return;
        }

        if (forAll) {
            storeVarOfAllData(url, user, password, stockSymbol, daysOfInvestment, meanReturn, volatility, var, initialStockPrice, confidenceLevel);
        } else {
            storeVaRData(url, user, password, stockSymbol, daysOfInvestment, meanReturn, volatility, var, initialStockPrice, confidenceLevel);
        }
    }

    public static List<Double> getClosePrices(String url, String user, String password, String stockSymbol) throws Exception {
        List<Double> closePrices = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement()) {
            String query = "SELECT close FROM daily_data_" + stockSymbol;
            ResultSet rs = stmt.executeQuery(query);

            while (rs.next()) {
                closePrices.add(rs.getDouble("close"));
            }
        }

        return closePrices;
    }

    public static double calculateMeanReturn(List<Double> prices, int days) {
        List<Double> returns = new ArrayList<>();
        for (int i = 1; i < days; i++) {
            double dailyReturn = Math.log(prices.get(prices.size() - i) / prices.get(prices.size() - i - 1));
            returns.add(dailyReturn);
        }
        return returns.stream().mapToDouble(r -> r).average().orElse(0.0);
    }

    public static double calculateVolatility(List<Double> prices, int days, double meanReturn) {
        List<Double> returns = new ArrayList<>();
        for (int i = 1; i < days; i++) {
            double dailyReturn = Math.log(prices.get(prices.size() - i) / prices.get(prices.size() - i - 1));
            returns.add(dailyReturn);
        }
        double variance = returns.stream().mapToDouble(r -> Math.pow(r - meanReturn, 2)).sum() / (returns.size() - 1);
        return Math.sqrt(variance);
    }

    public static double calculateVaR(double initialStockPrice, double meanReturn, double volatility, int daysOfInvestment,
                                      int numSimulations, double confidenceLevel) throws Exception {
        int numThreads = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        Future<double[]>[] futures = new Future[numThreads];

        int simulationsPerThread = numSimulations / numThreads;
        Random random = new Random();

        for (int i = 0; i < numThreads; i++) {
            futures[i] = executor.submit(new Callable<double[]>() {
                @Override
                public double[] call() {
                    double[] simulatedReturns = new double[simulationsPerThread];
                    for (int j = 0; j < simulationsPerThread; j++) {
                        simulatedReturns[j] = simulateReturn(initialStockPrice, meanReturn, volatility, daysOfInvestment, random);
                    }
                    return simulatedReturns;
                }
            });
        }

        double[] allSimulatedReturns = new double[numSimulations];
        int index = 0;

        for (Future<double[]> future : futures) {
            double[] simulatedReturns = future.get();
            System.arraycopy(simulatedReturns, 0, allSimulatedReturns, index, simulatedReturns.length);
            index += simulatedReturns.length;
        }

        executor.shutdown();

        Arrays.sort(allSimulatedReturns);
        int varIndex = (int) Math.max(0, (1 - confidenceLevel) * numSimulations - 1);
        double varReturn = allSimulatedReturns[varIndex];

        return initialStockPrice * -varReturn;
    }

    private static double simulateReturn(double initialStockPrice, double meanReturn, double volatility, int daysOfInvestment, Random random) {
        double dt = 1.0 / 174;
        double totalReturn = 0;

        for (int i = 0; i < daysOfInvestment; i++) {
            double drift = (meanReturn - 0.5 * Math.pow(volatility, 2)) * dt;
            double shock = volatility * Math.sqrt(dt) * random.nextGaussian();
            totalReturn += drift + shock;
        }

        return totalReturn;
    }

    private static void storeVaRData(String url, String user, String password, String stockSymbol, int daysOfInvestment,
                                     double meanReturn, double volatility, double var, double initialStockPrice, double confidenceLevel) throws Exception {
        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement()) {

            String createTableSQL = "CREATE TABLE IF NOT EXISTS var_data (" +
                    "id INT AUTO_INCREMENT PRIMARY KEY, " +
                    "stock_symbol VARCHAR(20), " +
                    "date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
                    "days_of_investment INT, " +
                    "mean_return DOUBLE, " +
                    "volatility DOUBLE, " +
                    "var DOUBLE, " +
                    "initial_stock_price DOUBLE," +
                    "confidence_level DOUBLE)";
            stmt.execute(createTableSQL);

            String insertSQL = "INSERT INTO var_data (stock_symbol, days_of_investment, mean_return, volatility, var, initial_stock_price, confidence_level) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                pstmt.setString(1, stockSymbol);
                pstmt.setInt(2, daysOfInvestment);
                pstmt.setDouble(3, meanReturn);
                pstmt.setDouble(4, volatility);
                pstmt.setDouble(5, var);
                pstmt.setDouble(6, initialStockPrice);
                pstmt.setDouble(7, confidenceLevel);
                pstmt.executeUpdate();
            }
        }
    }

    private static void storeVarOfAllData(String url, String user, String password, String stockSymbol, int daysOfInvestment,
                                          double meanReturn, double volatility, double var, double initialStockPrice, double confidenceLevel) throws Exception {
        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement()) {

            String createTableSQL = "CREATE TABLE IF NOT EXISTS varofall_data (" +
                    "id INT AUTO_INCREMENT PRIMARY KEY, " +
                    "stock_symbol VARCHAR(20), " +
                    "date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
                    "days_of_investment INT, " +
                    "mean_return DOUBLE, " +
                    "volatility DOUBLE, " +
                    "var DOUBLE, " +
                    "initial_stock_price DOUBLE," +
                    "confidence_level DOUBLE)";
            stmt.execute(createTableSQL);

            String insertSQL = "INSERT INTO varofall_data (stock_symbol, days_of_investment, mean_return, volatility, var, initial_stock_price, confidence_level) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                pstmt.setString(1, stockSymbol);
                pstmt.setInt(2, daysOfInvestment);
                pstmt.setDouble(3, meanReturn);
                pstmt.setDouble(4, volatility);
                pstmt.setDouble(5, var);
                pstmt.setDouble(6, initialStockPrice);
                pstmt.setDouble(7, confidenceLevel);
                pstmt.executeUpdate();
            }
        }
    }

    private static double calculateDynamicConfidenceLevel(String url, String user, String password, String stockSymbol) throws Exception {
        List<Double> confidenceLevels = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(url, user, password);
             PreparedStatement pstmt = conn.prepareStatement(
                     "SELECT confidence_level FROM varofall_data WHERE stock_symbol = ? ORDER BY date DESC LIMIT 30"
             )) {
            pstmt.setString(1, stockSymbol);
            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                confidenceLevels.add(rs.getDouble("confidence_level"));
            }
        }

        return confidenceLevels.stream().mapToDouble(Double::doubleValue).average().orElse(0.95);
    }
}
