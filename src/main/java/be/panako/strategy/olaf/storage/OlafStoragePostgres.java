/***************************************************************************
*                                                                          *
* Panako - acoustic fingerprinting                                         *
*                                                                          *
****************************************************************************/

package be.panako.strategy.olaf.storage;

import be.panako.util.Config;
import be.panako.util.Key;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.*;
import java.util.*;

/**
 * OlafStorage implementation backed by PostgreSQL.
 * Schema:
 *  - fingerprints(hash BIGINT NOT NULL, resource_id INT NOT NULL, t1 INT NOT NULL)
 *    Index: CREATE INDEX IF NOT EXISTS fp_hash_idx ON fingerprints(hash);
 *  - resource_metadata(resource_id INT PRIMARY KEY, path TEXT, duration REAL, num_fingerprints INT)
 */
public class OlafStoragePostgres implements OlafStorage {

    private static OlafStoragePostgres instance;
    private static final Object mutex = new Object();

    private final HikariDataSource dataSource;

    private final Map<Long, List<long[]>> storeQueue;
    private final Map<Long, List<long[]>> deleteQueue;
    private final Map<Long, List<Long>> queryQueue;

    public static synchronized OlafStoragePostgres getInstance() {
        if (instance == null) {
            instance = new OlafStoragePostgres();
        }
        return instance;
    }

    public OlafStoragePostgres() {
        String url = Config.get(Key.OLAF_POSTGRES_URL);
        String user = Config.get(Key.OLAF_POSTGRES_USER);
        String password = Config.get(Key.OLAF_POSTGRES_PASSWORD);
        if (url == null || url.trim().isEmpty()) {
            throw new RuntimeException("Missing or empty configuration for OLAF_POSTGRES_URL");
        }
        if (user == null || user.trim().isEmpty()) {
            throw new RuntimeException("Missing or empty configuration for OLAF_POSTGRES_USER");
        }
        if (password == null || password.trim().isEmpty()) {
            throw new RuntimeException("Missing or empty configuration for OLAF_POSTGRES_PASSWORD");
        }
        
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(password);
        config.setAutoCommit(false);
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(30000);
        config.setIdleTimeout(600000);
        config.setMaxLifetime(1800000);
        
        dataSource = new HikariDataSource(config);
        
        try (Connection conn = dataSource.getConnection()) {
            ensureSchema(conn);
        } catch (SQLException e) {
            throw new RuntimeException("Could not initialize database schema: " + e.getMessage(), e);
        }
        
        storeQueue = new HashMap<>();
        deleteQueue = new HashMap<>();
        queryQueue = new HashMap<>();
    }

    private void ensureSchema(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("CREATE TABLE IF NOT EXISTS fingerprints (" +
                    "hash BIGINT NOT NULL, " +
                    "resource_id INT NOT NULL, " +
                    "t1 INT NOT NULL) ");
            st.executeUpdate("CREATE INDEX IF NOT EXISTS fp_hash_idx ON fingerprints(hash)");
            st.executeUpdate("CREATE TABLE IF NOT EXISTS resource_metadata (" +
                    "resource_id INT PRIMARY KEY, " +
                    "path TEXT NOT NULL, " +
                    "duration REAL NOT NULL, " +
                    "num_fingerprints INT NOT NULL)");
            conn.commit();
        }
    }

    @Override
    public void storeMetadata(long resourceID, String resourcePath, float duration, int fingerprints) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO resource_metadata(resource_id, path, duration, num_fingerprints) " +
                        "VALUES(?,?,?,?) ON CONFLICT (resource_id) DO UPDATE SET path=EXCLUDED.path, duration=EXCLUDED.duration, num_fingerprints=EXCLUDED.num_fingerprints")) {
            ps.setInt(1, (int) resourceID);
            ps.setString(2, resourcePath);
            ps.setFloat(3, duration);
            ps.setInt(4, fingerprints);
            ps.executeUpdate();
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public OlafResourceMetadata getMetadata(long resourceID) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                "SELECT path, duration, num_fingerprints FROM resource_metadata WHERE resource_id = ?")) {
            ps.setInt(1, (int) resourceID);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    OlafResourceMetadata m = new OlafResourceMetadata();
                    m.identifier = (int) resourceID;
                    m.path = rs.getString(1);
                    m.duration = rs.getFloat(2);
                    m.numFingerprints = rs.getInt(3);
                    return m;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    @Override
    public void addToStoreQueue(long fingerprintHash, int resourceIdentifier, int t1) {
        long[] data = {fingerprintHash, resourceIdentifier, t1};
        long threadID = Thread.currentThread().getId();
        storeQueue.computeIfAbsent(threadID, k -> new ArrayList<>()).add(data);
    }

    @Override
    public void processStoreQueue() {
        long threadID = Thread.currentThread().getId();
        List<long[]> queue = storeQueue.get(threadID);
        if (queue == null || queue.isEmpty()) return;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO fingerprints(hash, resource_id, t1) VALUES(?,?,?)")) {
            for (long[] d : queue) {
                ps.setLong(1, d[0]);
                ps.setInt(2, (int) d[1]);
                ps.setInt(3, (int) d[2]);
                ps.addBatch();
            }
            ps.executeBatch();
            conn.commit();
            queue.clear();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void clearStoreQueue() {
        storeQueue.clear();
    }

    @Override
    public void addToDeleteQueue(long fingerprintHash, int resourceIdentifier, int t1) {
        long[] data = {fingerprintHash, resourceIdentifier, t1};
        long threadID = Thread.currentThread().getId();
        deleteQueue.computeIfAbsent(threadID, k -> new ArrayList<>()).add(data);
    }

    @Override
    public void processDeleteQueue() {
        long threadID = Thread.currentThread().getId();
        List<long[]> queue = deleteQueue.get(threadID);
        if (queue == null || queue.isEmpty()) return;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM fingerprints WHERE hash = ? AND resource_id = ? AND t1 = ?")) {
            for (long[] d : queue) {
                ps.setLong(1, d[0]);
                ps.setInt(2, (int) d[1]);
                ps.setInt(3, (int) d[2]);
                ps.addBatch();
            }
            ps.executeBatch();
            conn.commit();
            queue.clear();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addToQueryQueue(long queryHash) {
        long threadID = Thread.currentThread().getId();
        queryQueue.computeIfAbsent(threadID, k -> new ArrayList<>()).add(queryHash);
    }

    @Override
    public void processQueryQueue(Map<Long, List<OlafHit>> matchAccumulator, int range, Set<Integer> resourcesToAvoid) {
        long threadID = Thread.currentThread().getId();
        List<Long> queue = queryQueue.get(threadID);
        if (queue == null || queue.isEmpty()) return;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                "SELECT hash, resource_id, t1 FROM fingerprints WHERE hash BETWEEN ? AND ? ORDER BY hash")) {
            for (long originalKey : queue) {
                long startKey = originalKey - range;
                long stopKey = originalKey + range;
                ps.setLong(1, startKey);
                ps.setLong(2, stopKey);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        long matchedHash = rs.getLong(1);
                        int resourceId = rs.getInt(2);
                        int t1 = rs.getInt(3);
                        if (!resourcesToAvoid.contains(resourceId)) {
                            matchAccumulator.computeIfAbsent(originalKey, k -> new ArrayList<>())
                                    .add(new OlafHit(originalKey, matchedHash, t1, resourceId));
                        }
                    }
                }
            }
            conn.commit();
            queue.clear();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void printStatistics(boolean printDetailedStats) {
        try (Connection conn = dataSource.getConnection();
             Statement st = conn.createStatement()) {
            long count;
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM fingerprints")) {
                rs.next();
                count = rs.getLong(1);
            }
            System.out.printf("[Postgres INDEX TOTALS]\n");
            System.out.printf("=========================\n");
            System.out.printf("> %d fingerprint hashes \n", count);
            System.out.printf("=========================\n\n");

            if (printDetailedStats) {
                long resources = 0;
                double totalDuration = 0;
                long totalPrints = 0;
                double maxPps = 0;
                String maxPpsPath = "";
                double minPps = Double.MAX_VALUE;
                String minPpsPath = "";

                try (ResultSet rs = st.executeQuery("SELECT resource_id, path, duration, num_fingerprints FROM resource_metadata")) {
                    while (rs.next()) {
                        int rid = rs.getInt(1);
                        String path = rs.getString(2);
                        double dur = rs.getDouble(3);
                        int nfp = rs.getInt(4);
                        resources++;
                        totalDuration += dur;
                        totalPrints += nfp;
                        double pps = nfp / Math.max(dur, 1e-9);
                        if (pps > maxPps) { maxPps = pps; maxPpsPath = path; }
                        if (pps < minPps) { minPps = pps; minPpsPath = path; }
                    }
                }
                double avgPps = totalDuration > 0 ? totalPrints / totalDuration : 0;
                System.out.printf("[Postgres INDEX INFO]\n");
                System.out.printf("=========================\n");
                System.out.printf("> %d audio files \n", resources);
                System.out.printf("> %.3f seconds of audio\n", totalDuration);
                System.out.printf("> %d fingerprint hashes \n", totalPrints);
                System.out.printf("> Avg prints per second: %5.1ffp/s \n", avgPps);
                System.out.printf("> Min prints per second: %5.1ffp/s '%s'\n", minPps, minPpsPath);
                System.out.printf("> Max prints per second: %5.1ffp/s '%s'\n", maxPps, maxPpsPath);
                System.out.printf("=========================\n\n");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteMetadata(long resourceID) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM resource_metadata WHERE resource_id = ?")) {
            ps.setInt(1, (int) resourceID);
            ps.executeUpdate();
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void clear() {
        try (Connection conn = dataSource.getConnection();
             Statement st = conn.createStatement()) {
            st.executeUpdate("TRUNCATE TABLE fingerprints");
            st.executeUpdate("TRUNCATE TABLE resource_metadata");
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
