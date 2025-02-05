
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DataTransferService {

    private static final String DB_URL = "jdbc:mysql://localhost:3306/osius";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "raja2103";

    public static void main(String[] args) {
        DataTransferService service = new DataTransferService();
        service.generateInsertScripts();
    }

    private char generateRandomAlphabet() {
        return (char) ('A' + new Random().nextInt(26));
    }

    private int generateRandomNumber() {
        return new Random().nextInt(10);
    }

    public void generateInsertScripts() {
        Connection connection = null;
        Statement statement = null;

        try {
            connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            statement = connection.createStatement();

            String selectEmployeesQuery = "SELECT id, fname, lname, pan FROM osius.employees";
            ResultSet employeesResultSet = statement.executeQuery(selectEmployeesQuery);

            List<EmployeeData> employees = new ArrayList<>();
            while (employeesResultSet.next()) {
                employees.add(new EmployeeData(
                        employeesResultSet.getInt("id"),
                        employeesResultSet.getString("fname"),
                        employeesResultSet.getString("lname"),
                        employeesResultSet.getString("pan")
                ));
            }

            String selectMappingQuery = "SELECT * FROM osius.mappingtable";
            ResultSet mappingsResultSet = statement.executeQuery(selectMappingQuery);

            List<MappingRule> mappings = new ArrayList<>();
            while (mappingsResultSet.next()) {
                mappings.add(new MappingRule(
                        mappingsResultSet.getString("source_column_name"),
                        mappingsResultSet.getBoolean("is_secure"),
                        mappingsResultSet.getString("pattern")
                ));
            }


            StringBuilder insertScripts = new StringBuilder();

            for (MappingRule mapping : mappings) {
                if (!mapping.isSecure && "pan".equals(mapping.sourceColumn)) {
                    for (EmployeeData employee : employees) {
                        String transformedPan = applyPattern(employee.pan, mapping.pattern);
                        if (transformedPan != null && !transformedPan.equals(employee.pan)) {
                            String sql = String.format(
                                    "INSERT INTO osius.emp (first_name, last_name, pan_number) VALUES ('%s', '%s', '%s');",
                                    escapeSQL(employee.firstName),
                                    escapeSQL(employee.lastName),
                                    escapeSQL(transformedPan)
                            );
                            insertScripts.append(sql).append("\n");
                        }
                    }
                }
            }


            System.out.println("Generated INSERT Scripts:");
            System.out.println(insertScripts.toString());

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (statement != null) statement.close();
                if (connection != null) connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private String applyPattern(String value, String pattern) {
        if (value == null || pattern == null) {
            return value;
        }

        StringBuilder result = new StringBuilder();
        for (char ch : pattern.toCharArray()) {
            if (ch == '$') {
                result.append(generateRandomAlphabet());
            } else if (ch == '#') {
                result.append(generateRandomNumber());
            } else {
                result.append(ch);
            }
        }
        return result.toString();
    }

    private String escapeSQL(String value) {
        return value == null ? "" : value.replace("'", "''"); // Escape single quotes for SQL
    }

    private static class EmployeeData {
        int id;
        String firstName;
        String lastName;
        String pan;

        EmployeeData(int id, String firstName, String lastName, String pan) {
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
            this.pan = pan;
        }
    }

    private static class MappingRule {
        String sourceColumn;
        boolean isSecure;
        String pattern;

        MappingRule(String sourceColumn, boolean isSecure, String pattern) {
            this.sourceColumn = sourceColumn;
            this.isSecure = isSecure;
            this.pattern = pattern;
        }
    }
}

