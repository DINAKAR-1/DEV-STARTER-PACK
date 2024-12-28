package in.gov.cgg.scheduler.paymentFiles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class DatabaseHelper {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseHelper.class);

    @Autowired
    private DataSource dataSource;

    /**
     * Fetches a string value from the database using the provided query and parameters.
     * 
     * @param query  SQL query to execute
     * @param params parameters for the query
     * @return the string result or null if not found
     */
    public String getStringFromQuery(String query, Object[] params) {
        String result = null;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(query)) {

            setParameters(stmt, params);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    result = rs.getString(1); // Assuming the first column is the string we want
                }
            }
        } catch (SQLException e) {
            logger.error("Error fetching string from query: {}", query, e);
        }

        return result;
    }

    /**
     * Checks if a row exists based on the provided query and parameters.
     * 
     * @param query  SQL query to execute
     * @param params parameters for the query
     * @return true if the row exists, false otherwise
     */
    public boolean isExist(String query, Object[] params) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(query)) {

            setParameters(stmt, params);

            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            logger.error("Error checking existence with query: {}", query, e);
            return false;
        }
    }

    /**
     * Executes the provided query and returns a list of results as maps.
     * Each map represents a row with column names as keys and the column values as values.
     * 
     * @param query  SQL query to execute
     * @param params parameters for the query
     * @return List of maps representing the result set
     * @throws SQLException if there's an issue executing the query
     */
    public List<Map<String, Object>> executeQuery(String query, Object[] params) throws SQLException {
        List<Map<String, Object>> results = new ArrayList<>();

        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(query)) {

            setParameters(stmt, params);

            try (ResultSet rs = stmt.executeQuery()) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnLabel(i);
                        Object columnValue = rs.getObject(i);
                        row.put(columnName, columnValue);
                    }
                    results.add(row);
                }
            }
        }

        return results;
    }

    /**
     * Executes the provided update query (INSERT, UPDATE, DELETE).
     * 
     * @param query  SQL query to execute
     * @param params parameters for the query
     * @return number of rows affected by the query
     */
    public int executeUpdate(String query, Object[] params) {
        int rowsAffected = 0;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(query)) {

            setParameters(stmt, params);
            rowsAffected = stmt.executeUpdate();
        } catch (SQLException e) {
            logger.error("Error executing update query: {}", query, e);
        }
        return rowsAffected;
    }

    /**
     * Executes a batch of SQL queries (INSERT, UPDATE, DELETE).
     * 
     * @param sqls list of SQL queries to execute
     * @param batchSize the size of the batch for optimal performance
     * @return the number of rows affected by the batch execution
     */
    @Transactional
    public int executeBatch(List<String> sqls, int batchSize) {
        if (sqls == null || sqls.isEmpty()) {
            throw new IllegalArgumentException("executeBatch(): NULL or empty SQLs");
        }

        int result = 0;
        try (Connection con = dataSource.getConnection();
             Statement st = con.createStatement()) {

            int batchCount = 0;
            for (String sql : sqls) {
                st.addBatch(sql);
                batchCount++;

                if (batchCount % batchSize == 0) {
                    int[] count = st.executeBatch();
                    if (count != null) {
                        result = 1; // Success
                    }
                }
            }

            // Execute remaining queries
            int[] count = st.executeBatch();
            if (count != null) {
                result = 1; // Success
            }
        } catch (SQLException e) {
            logger.error("Error executing batch", e);
            result = 0; // Failure
        }

        return result;
    }

    /**
     * Helper method to set parameters for PreparedStatement.
     * 
     * @param stmt   PreparedStatement to set the parameters on
     * @param params parameters to set
     * @throws SQLException if there's an issue setting parameters
     */
    private void setParameters(PreparedStatement stmt, Object[] params) throws SQLException {
        if (params != null) {
            for (int i = 0; i < params.length; i++) {
                stmt.setObject(i + 1, params[i]);
            }
        }
    }

    /**
     * Closes the provided resources.
     * 
     * @param st Statement to close
     * @param con Connection to close
     */
    private void closeResources(Statement st, Connection con) {
        try {
            if (st != null) {
                st.close();
            }
            if (con != null) {
                con.close();
            }
        } catch (SQLException e) {
            logger.error("Error closing resources", e);
        }
    }
}
