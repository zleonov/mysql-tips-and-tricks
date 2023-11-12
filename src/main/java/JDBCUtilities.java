import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

public class JDBCUtilities {

    private JDBCUtilities() {
    }

    public static boolean checkAndUpdate(final ResultSet rs, final int idx, final int n) throws SQLException {
        if (n != rs.getInt(idx)) {
            rs.updateInt(idx, n);
            return true;
        }
        return false;
    }

    public static boolean checkAndUpdate(final ResultSet rs, final int idx, final LocalDate date) throws SQLException {
        if (!date.equals(rs.getDate(idx).toLocalDate())) {
            rs.updateObject(idx, date);
            return true;
        }
        return false;
    }

    public static boolean checkAndUpdate(final ResultSet rs, final int idx, final String str) throws SQLException {
        if (!str.equals(rs.getString(idx))) {
            rs.updateString(idx, str);
            return true;
        }
        return false;
    }

    public static PreparedStatement setParameters(final PreparedStatement pstmt, final Object... values) throws SQLException {
        return setParameters(pstmt, Arrays.asList(values));
    }

    public static PreparedStatement setParameters(final PreparedStatement pstmt, final List<Object> values) throws SQLException {
        int idx = 1;
        for (final Object value : values)
            pstmt.setObject(idx++, value); // arbitrary penalty compared to using specific setXXXX methods
        return pstmt;
    }

    public static void truncateTables(final Connection conn, final String... tables) throws SQLException, IOException {
        try (final Statement stmt = conn.createStatement()) {
            stmt.execute("SET FOREIGN_KEY_CHECKS=0");

            for (final String table : tables) {
                System.out.println("TRUNCATE " + table);
                stmt.execute("TRUNCATE " + table);
            }

            stmt.execute("SET FOREIGN_KEY_CHECKS=1");
        }
    }
}
