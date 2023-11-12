import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;

public class FastImporter {

    private static final Splitter SPLITTER = Splitter.on(',');

    private static final Path CSV_DIR = Paths.get("res/small_csv");

    private static final String DATABASE       = "jdbc:mysql://localhost:3306/company?rewriteBatchedStatements=true&useServerPrepStmts=false";
    private static final String USERNAME       = "root";
    private static final String PASSWORD       = "root";
    private static final int    MAX_BATCH_SIZE = 500;

    public static void main(String... args) throws IOException, SQLException {

        final Stopwatch watch = Stopwatch.createUnstarted();

        System.out.println(String.format("Connecting to %s\n", DATABASE));

        try (final Connection conn = DriverManager.getConnection(DATABASE, USERNAME, PASSWORD)) {

           // JDBCUtilities.truncateTables(conn, "dept_emp", "emp_salary", "departments", "employees");

            watch.start();

            conn.setAutoCommit(false);

            importDepartments(conn);
            importEmployees(conn);
            importDeptEmp(conn);
            importEmpSalary(conn);

            conn.commit();

            watch.stop();

            printStats(conn);
        }

        System.out.println(String.format("Finished in %s seconds", watch.elapsed(TimeUnit.SECONDS)));
    }

    private static void importDepartments(final Connection conn) throws SQLException, IOException {
        importTable(conn, "departments.csv", "INSERT IGNORE INTO departments (dept_name) VALUES (?)", (parts) -> {
            final String name = parts.get(0);
            return Arrays.asList(name);
        });
    }

    private static void importEmployees(final Connection conn) throws SQLException, IOException {
        importTable(conn, "employees.csv",
                "INSERT INTO employees (first_name, last_name, birth_date, gender, hire_date) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE birth_date=VALUES(birth_date), gender=VALUES(gender), hire_date=VALUES(hire_date)", (parts) -> {
                    final String first = parts.get(0);
                    final String last = parts.get(1);
                    final LocalDate birthday = LocalDate.parse(parts.get(2));
                    final String gender = parts.get(3);
                    final LocalDate hired = LocalDate.parse(parts.get(4));
                    return Arrays.asList(first, last, birthday, gender, hired);
                });
    }

    private static void importDeptEmp(final Connection conn) throws SQLException, IOException {
        importTable(conn, "dept_emp.csv",
                "INSERT INTO dept_emp (dept_id, emp_id, from_date, to_date) VALUES ((SELECT id FROM departments WHERE dept_name=?), (SELECT id FROM employees WHERE first_name=? AND last_name=?), ?, ?) ON DUPLICATE KEY UPDATE from_date=VALUES(from_date), to_date=VALUES(to_date)",
                (parts) -> {
                    final String dept = parts.get(0);
                    final String first = parts.get(1);
                    final String last = parts.get(2);
                    final LocalDate from = LocalDate.parse(parts.get(3));
                    final LocalDate to = LocalDate.parse(parts.get(4));
                    return Arrays.asList(dept, first, last, from, to);
                });
    }

    private static void importEmpSalary(final Connection conn) throws SQLException, IOException {
        importTable(conn, "emp_salary.csv",
                "INSERT INTO emp_salary (emp_id, salary, from_date, to_date) VALUES ((SELECT id FROM employees WHERE first_name=? AND last_name=?), ?, ?, ?) ON DUPLICATE KEY UPDATE salary=VALUES(salary), to_date=VALUES(to_date)",
                (parts) -> {
                    final String first = parts.get(0);
                    final String last = parts.get(1);
                    final int salary = Integer.parseInt(parts.get(2));
                    final LocalDate from = LocalDate.parse(parts.get(3));
                    final LocalDate to = LocalDate.parse(parts.get(4));
                    return Arrays.asList(first, last, salary, from, to);
                });
    }

    private static void importTable(final Connection conn, final String filename, final String sql, final Function<List<String>, List<Object>> func) throws SQLException, IOException {
        final Path csv = Paths.get(CSV_DIR + "/" + filename);

        System.out.println(String.format("Importing %s", csv));

        int count = 0;

        try (final BufferedReader reader = Files.newBufferedReader(csv); final PreparedStatement pstmt = conn.prepareStatement(sql)) {

            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                if (line.startsWith("#"))
                    continue;

                final List<String> parts  = SPLITTER.splitToList(line);
                final List<Object> values = func.apply(parts);

                JDBCUtilities.setParameters(pstmt, values).addBatch();

                count++;
                if (count % MAX_BATCH_SIZE == 0) {
                    System.out.println(String.format("Processed %s rows: executing batch", count));
                    pstmt.executeBatch();
                }
            }

            if (count % MAX_BATCH_SIZE != 0) {
                System.out.println(String.format("Processed %s rows: executing batch", count));
                pstmt.executeBatch();

            }

            System.out.println(String.format("Finished importing %s\n", csv));
        }
    }

    // @formatter:off
    private static final String STATS_QUERY =
            "SELECT 'departments', (SELECT COUNT(*) FROM departments) 'total_records', (SELECT COUNT(*) FROM departments WHERE last_update_date IS NOT NULL) 'updated_records'"
          + "UNION ALL "
          + "SELECT 'employees', (SELECT COUNT(*) FROM employees), (SELECT COUNT(*) FROM employees WHERE last_update_date IS NOT NULL)"
          + "UNION ALL "
          + "SELECT 'dept_emp', (SELECT COUNT(*) FROM dept_emp), (SELECT COUNT(*) FROM dept_emp WHERE last_update_date IS NOT NULL)"
          + "UNION ALL "
          + "SELECT 'emp_salary', (SELECT COUNT(*) FROM emp_salary), (SELECT COUNT(*) FROM emp_salary WHERE last_update_date IS NOT NULL)";            
    // @formatter:on

    private static void printStats(final Connection conn) throws SQLException {

        System.out.println(String.format("|-------------|---------------|-----------------|"));
        System.out.println(String.format("| %s | %s | %s |", new Object[] { "table      ", "total records", "updated records", 7 }));
        System.out.println(String.format("|-------------|---------------|-----------------|"));
        try (final Statement stmt = conn.createStatement(); final ResultSet rs = stmt.executeQuery(STATS_QUERY)) {
            while (rs.next()) {
                final String  table   = rs.getString(1);
                final Integer total   = rs.getInt(2);
                final Integer updated = rs.getInt(3);
                System.out.println(String.format("| %s | %s | %s |", new Object[] { Strings.padEnd(table, 11, ' '), Strings.padEnd(total.toString(), 13, ' '), Strings.padEnd(updated.toString(), 15, ' ') }));
                System.out.println(String.format("|-------------|---------------|-----------------|"));
            }
        }
    }

}