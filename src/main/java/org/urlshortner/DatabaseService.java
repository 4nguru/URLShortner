package org.urlshortner;

import java.sql.*;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcConnectionPool;

public class DatabaseService {
    private static final String DB_URL = "jdbc:h2:mem:urlshortener;DB_CLOSE_DELAY=-1";
    private final DataSource dataSource;
    
    public DatabaseService() {
        try {
            // Create connection pool for thread safety
            this.dataSource = JdbcConnectionPool.create(DB_URL, "sa", "");
            createTable();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize database", e);
        }
    }
    
    private void createTable() throws SQLException {
        String sql = """
            CREATE TABLE IF NOT EXISTS urls (
                short_code VARCHAR(10) PRIMARY KEY,
                original_url TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """;
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }
    
    public boolean storeMapping(String shortCode, String originalUrl) {
        String sql = "INSERT INTO urls (short_code, original_url) VALUES (?, ?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, shortCode);
            stmt.setString(2, originalUrl);
            stmt.executeUpdate();
            return true;
        } catch (SQLException e) {
            // Handle unique constraint violation (collision)
            if (e.getErrorCode() == 23505) { // H2 unique constraint violation
                return false;
            }
            throw new RuntimeException("Database error", e);
        }
    }
    
    public String getOriginalUrl(String shortCode) {
        String sql = "SELECT original_url FROM urls WHERE short_code = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, shortCode);
            ResultSet rs = stmt.executeQuery();
            return rs.next() ? rs.getString("original_url") : null;
        } catch (SQLException e) {
            throw new RuntimeException("Database error", e);
        }
    }
    
    public boolean shortCodeExists(String shortCode) {
        return getOriginalUrl(shortCode) != null;
    }
}