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
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;

public class SimpleImporter {
    private static final Splitter SPLITTER = Splitter.on(',');

    private static final Path CSV_DIR = Paths.get("res/small_csv");

    private static final String DATABASE = "jdbc:mysql://localhost:3306/company";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";

    private static final int STEP_SIZE = 500; // progress logging step size

    public static void main(String... args) throws IOException, SQLException {

        final Stopwatch watch = Stopwatch.createUnstarted();

        System.out.println(String.format("Connecting to %s\n", DATABASE));

        try (final Connection conn = DriverManager.getConnection(DATABASE, USERNAME, PASSWORD)) {
            JDBCUtilities.truncateTables(conn, "dept_emp", "emp_salary", "departments", "employees");

            watch.start();

            importDepartments(conn);
            importEmployees(conn);
            importDeptEmp(conn);
            importEmpSalary(conn);

            watch.stop();

            printStats(conn);
        }

        System.out.println(String.format("Finished in %s seconds", watch.elapsed(TimeUnit.SECONDS)));
    }

    private static void importDepartments(final Connection conn) throws SQLException, IOException {
        final Path csv = Paths.get(CSV_DIR + "/departments.csv");

        System.out.println(String.format("Importing %s", csv));

        int count = 0;

        // @formatter:off
        try (final BufferedReader        reader = Files.newBufferedReader(csv);
             final PreparedStatement selectDept = conn.prepareStatement("SELECT dept_name FROM departments WHERE dept_name=?");
             final PreparedStatement      pstmt = conn.prepareStatement("INSERT INTO departments (dept_name) VALUES (?)")) {
        // @formatter:on

            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                if (line.startsWith("#"))
                    continue;

                selectDept.setString(1, line);
                try (final ResultSet rs = selectDept.executeQuery()) {
                    if (!rs.next()) {
                        pstmt.setString(1, line);
                        pstmt.execute();
                    }
                }

                count++;

                if (count % STEP_SIZE == 0)
                    System.out.println(String.format("Processed %s rows", count));
            }

            if (count % STEP_SIZE != 0)
                System.out.println(String.format("Processed %s rows", count));

            System.out.println(String.format("Finished importing %s\n", csv));
        }
    }

    private static void importEmployees(final Connection conn) throws SQLException, IOException {
        final Path csv = Paths.get(CSV_DIR + "/employees.csv");

        System.out.println(String.format("Importing %s", csv));

        int count = 0;

        // @formatter:off
        try (final BufferedReader       reader = Files.newBufferedReader(csv);
             final PreparedStatement selectEmp = conn.prepareStatement("SELECT * FROM employees WHERE first_name=? AND last_name=?", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
             final PreparedStatement     pstmt = conn.prepareStatement("INSERT INTO employees (first_name, last_name, birth_date, gender, hire_date) VALUES (?, ?, ?, ?, ?)")) {
        // @formatter:on

            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                if (line.startsWith("#"))
                    continue;

                final List<String> parts    = SPLITTER.splitToList(line);
                final String       first    = parts.get(0);
                final String       last     = parts.get(1);
                final LocalDate    birthday = LocalDate.parse(parts.get(2));
                final String       gender   = parts.get(3);
                final LocalDate    hired    = LocalDate.parse(parts.get(4));

                JDBCUtilities.setParameters(selectEmp, first, last);

                try (final ResultSet rs = selectEmp.executeQuery()) {
                    if (rs.next()) {
                        boolean updated = false;

                        updated |= JDBCUtilities.checkAndUpdate(rs, 4, birthday);
                        updated |= JDBCUtilities.checkAndUpdate(rs, 5, gender);
                        updated |= JDBCUtilities.checkAndUpdate(rs, 6, hired);

                        if (updated) {
                            rs.updateTimestamp(8, Timestamp.from(Instant.now()));
                            rs.updateRow();
                        }
                    } else
                        JDBCUtilities.setParameters(pstmt, first, last, birthday, gender, hired).execute();
                }

                count++;

                if (count % STEP_SIZE == 0)
                    System.out.println(String.format("Processed %s rows", count));
            }

            if (count % STEP_SIZE != 0)
                System.out.println(String.format("Processed %s rows", count));

            System.out.println(String.format("Finished importing %s\n", csv));
        }
    }

    private static void importDeptEmp(final Connection conn) throws SQLException, IOException {
        final Path csv = Paths.get(CSV_DIR + "/dept_emp.csv");

        System.out.println(String.format("Importing %s", csv));

        int count = 0;

        // @formatter:off
        try (final BufferedReader           reader = Files.newBufferedReader(csv);
             final PreparedStatement  selectDeptId = conn.prepareStatement("SELECT id FROM departments WHERE dept_name=?");
             final PreparedStatement   selectEmpId = conn.prepareStatement("SELECT id FROM employees WHERE first_name=? AND last_name=?");
             final PreparedStatement selectDeptEmp = conn.prepareStatement("SELECT *  FROM dept_emp WHERE dept_id=? AND emp_id=?", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
             final PreparedStatement insertDeptEmp = conn.prepareStatement("INSERT INTO dept_emp (dept_id, emp_id, from_date, to_date) VALUES (?, ?, ?, ?)")) {
        // @formatter:on

            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                if (line.startsWith("#"))
                    continue;

                final List<String> parts = SPLITTER.splitToList(line);
                final String       dept  = parts.get(0);
                final String       first = parts.get(1);
                final String       last  = parts.get(2);
                final LocalDate    from  = LocalDate.parse(parts.get(3));
                final LocalDate    to    = LocalDate.parse(parts.get(4));

                selectDeptId.setString(1, dept);
                JDBCUtilities.setParameters(selectEmpId, first, last);

                try (final ResultSet deptIdRs = selectDeptId.executeQuery(); final ResultSet empIdRs = selectEmpId.executeQuery()) {
                    deptIdRs.next();
                    empIdRs.next();
                    final int deptId = deptIdRs.getInt(1);
                    final int empId  = empIdRs.getInt(1);

                    JDBCUtilities.setParameters(selectDeptEmp, deptId, empId);

                    try (final ResultSet rs = selectDeptEmp.executeQuery()) {
                        if (rs.next()) {
                            boolean updated = false;

                            updated |= JDBCUtilities.checkAndUpdate(rs, 4, from);
                            updated |= JDBCUtilities.checkAndUpdate(rs, 5, to);

                            if (updated) {
                                rs.updateObject(7, Timestamp.from(Instant.now()));
                                rs.updateRow();
                            }
                        } else
                            JDBCUtilities.setParameters(insertDeptEmp, deptId, empId, from, to).execute();
                    }
                }

                count++;

                if (count % STEP_SIZE == 0)
                    System.out.println(String.format("Processed %s rows", count));
            }

            if (count % STEP_SIZE != 0)
                System.out.println(String.format("Processed %s rows", count));

            System.out.println(String.format("Finished importing %s\n", csv));
        }
    }

    private static void importEmpSalary(final Connection conn) throws SQLException, IOException {
        final Path csv = Paths.get(CSV_DIR + "/emp_salary.csv");

        System.out.println(String.format("Importing %s", csv));

        int count = 0;

        // @formatter:off
        try (final BufferedReader             reader = Files.newBufferedReader(csv);
             final PreparedStatement     selectEmpId = conn.prepareStatement("SELECT id FROM employees WHERE first_name=? AND last_name=?");
             final PreparedStatement selectEmpSalary = conn.prepareStatement("SELECT * FROM emp_salary WHERE emp_id=? AND from_date=?", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
             final PreparedStatement insertEmpSalary = conn.prepareStatement("INSERT INTO emp_salary (emp_id, salary, from_date, to_date) VALUES (?, ?, ?, ?)")) {
        // @formatter:on

            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                if (line.startsWith("#"))
                    continue;

                final List<String> parts  = SPLITTER.splitToList(line);
                final String       first  = parts.get(0);
                final String       last   = parts.get(1);
                final int          salary = Integer.parseInt(parts.get(2));
                final LocalDate    from   = LocalDate.parse(parts.get(3));
                final LocalDate    to     = LocalDate.parse(parts.get(4));

                JDBCUtilities.setParameters(selectEmpId, first, last);

                try (final ResultSet empIdRs = selectEmpId.executeQuery()) {
                    empIdRs.next();
                    final int empId = empIdRs.getInt(1);

                    JDBCUtilities.setParameters(selectEmpSalary, empId, from);

                    try (final ResultSet rs = selectEmpSalary.executeQuery()) {
                        if (rs.next()) {
                            boolean updated = false;

                            updated |= JDBCUtilities.checkAndUpdate(rs, 3, salary);
                            updated |= JDBCUtilities.checkAndUpdate(rs, 5, to);

                            if (updated) {
                                rs.updateObject(7, Timestamp.from(Instant.now()));
                                rs.updateRow();
                            }
                        } else
                            JDBCUtilities.setParameters(insertEmpSalary, empId, salary, from, to).execute();
                    }
                }

                count++;

                if (count % STEP_SIZE == 0)
                    System.out.println(String.format("Processed %s rows", count));
            }

            if (count % STEP_SIZE != 0)
                System.out.println(String.format("Processed %s rows", count));

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