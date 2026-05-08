package org.drools.ansible.rulebook.integration.ha.tests;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Investigative test: H2 file-backed database behavior after process kill (SIGKILL).
 *
 * <h2>Background</h2>
 * In ansible-rulebook CI, HA failover is tested by:
 * <ol>
 *   <li>Starting the ansible-rulebook process (which embeds the drools jar with H2)</li>
 *   <li>Sending events so partial matches are persisted to H2</li>
 *   <li>Killing the process (simulating crash)</li>
 *   <li>Restarting the process with the SAME H2 file to verify recovery</li>
 * </ol>
 * Step 4 can fail with "Database may be already in use" because the killed process
 * never ran H2's SHUTDOWN command, potentially leaving a stale lock.
 *
 * <h2>Test approach</h2>
 * Each test spawns a child JVM that opens an H2 file-backed DB, writes data with
 * CHECKPOINT SYNC (to guarantee data is on disk), then gets forcibly killed via
 * destroyForcibly() (SIGKILL on Linux). The parent JVM then attempts to reopen the
 * same database file using different JDBC URL options. Each test method uses a unique
 * file path to avoid cross-test interference.
 *
 * <h2>Root cause analysis</h2>
 * H2's default FILE_LOCK=FILE uses two locking mechanisms:
 * <ul>
 *   <li><b>OS-level lock</b>: Java's {@code FileLock} (maps to {@code fcntl} on Linux).
 *       The OS releases this when the process dies.</li>
 *   <li><b>Lock file</b>: H2 creates a {@code .lock.db} file containing the PID and
 *       a heartbeat timestamp. On next open, H2 checks whether the PID is still alive.
 *       This heuristic can fail in container/CI environments where:
 *       <ul>
 *         <li>The new process cannot access /proc of the killed process (PID namespace isolation)</li>
 *         <li>PID reuse happens quickly (low PID namespaces in containers)</li>
 *         <li>The filesystem is shared/networked (NFS, CIFS) and the lock file is stale</li>
 *       </ul>
 *       This is the likely cause of Madhu's CI failures — not the OS-level lock, but H2's
 *       lock-file stale detection failing in containers.</li>
 * </ul>
 *
 * <h2>Results (tested on Linux with H2 2.2.232)</h2>
 * <ul>
 *   <li><b>Default lock (FILE_LOCK=FILE)</b>: On Linux with local filesystem, the OS releases
 *       the fcntl lock and H2 detects the stale .lock.db file via PID check. Recovery succeeds.
 *       In container/CI environments, the PID check may fail, causing "Database may be already
 *       in use".</li>
 *   <li><b>FILE_LOCK=NO</b>: Skips both the OS lock and the .lock.db file entirely. Always works
 *       after SIGKILL regardless of OS, filesystem, or container environment. Safe when only one
 *       process accesses the file at a time (guaranteed by ansible-rulebook's sequential
 *       kill-restart test). Trade-off: no protection against accidental concurrent access (silent
 *       corruption). Acceptable since H2 is a non-production path (code warns "not suitable for
 *       production").</li>
 *   <li><b>AUTO_SERVER=TRUE</b>: Starts an embedded TCP server so multiple JVMs can share the DB.
 *       After SIGKILL, the TCP port is released by the OS and the next process starts a fresh
 *       server. Works, but is heavier: opens a random TCP port (security concern in containers/CI),
 *       adds TCP overhead, and solves concurrent access — a problem that doesn't exist in the
 *       sequential kill-restart scenario.</li>
 *   <li><b>Data flushing (CHECKPOINT SYNC)</b>: H2's MVStore uses lazy flushing by default. A JDBC
 *       {@code commit()} guarantees durability within H2's engine, but not necessarily an immediate
 *       fsync. H2 has WAL (write-ahead log) recovery that replays committed transactions on next
 *       open, so committed data generally survives SIGKILL. The CHECKPOINT SYNC in this test is
 *       belt-and-suspenders to eliminate any timing window. Note: {@code H2StateManager} does NOT
 *       issue CHECKPOINT SYNC after writes — it relies on H2's WAL recovery, which is sufficient
 *       for the HA use case where multiple writes occur over seconds before a crash.</li>
 * </ul>
 *
 * <h2>Recommendation</h2>
 * For ansible-rulebook CI kill-restart scenarios, FILE_LOCK=NO is the simplest and most portable
 * fix. It has no external dependencies (no TCP port), works on all OSes, filesystems, and
 * container environments, and the single-process-at-a-time constraint is guaranteed by the test
 * harness. AUTO_SERVER=TRUE also works but is overkill for this use case.
 *
 * <p>This test is {@code @Disabled} because it is an investigative/exploratory test, not a
 * regression test. It spawns child JVM processes, uses Thread.sleep, and opens TCP ports
 * (AUTO_SERVER), making it unsuitable for CI. Run manually to verify H2 behavior on a
 * specific OS/environment.</p>
 */
@Disabled("Exploratory test for H2 lock behavior after SIGKILL - not for CI. Run manually.")
class H2FileLockAfterKillTest {

    private static final String DB_DIR = "./target/h2-kill-test";

    private String dbFilePath;

    @BeforeEach
    void setUp() throws Exception {
        // Unique file path per test method to avoid cross-test interference
        dbFilePath = DB_DIR + "/eda_ha_" + UUID.randomUUID().toString().substring(0, 8);
        Files.createDirectories(Path.of(DB_DIR));
    }

    @AfterEach
    void tearDown() throws Exception {
        // Clean up all files matching this test's db path
        Path dir = Path.of(DB_DIR);
        if (Files.exists(dir)) {
            String baseName = Path.of(dbFilePath).getFileName().toString();
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, baseName + ".*")) {
                for (Path file : stream) {
                    Files.deleteIfExists(file);
                }
            }
        }
    }

    /**
     * Tests default lock mode (FILE_LOCK=FILE) after SIGKILL.
     *
     * On Linux with local filesystem, the OS releases the fcntl lock on process death,
     * and H2 detects the stale .lock.db via PID check. Recovery succeeds.
     *
     * In container/CI environments, the PID check may fail (PID namespace isolation,
     * /proc not accessible), causing "Database may be already in use". This test documents
     * the OS-dependent behavior — it does NOT assert failure since the result varies.
     */
    @Test
    void testDefaultLockAfterKill() throws Exception {
        String jdbcUrl = "jdbc:h2:file:" + dbFilePath + ";MODE=PostgreSQL";

        spawnAndKillChildProcess(jdbcUrl);

        Path mvFile = Path.of(dbFilePath + ".mv.db");
        assertThat(mvFile).exists();

        Exception caughtException = null;
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "sa", "");
             Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM test_data");
            rs.next();
            int count = rs.getInt(1);
            System.out.println("Default lock: Successfully opened DB, row count = " + count);
            assertThat(count).isEqualTo(1);
        } catch (Exception e) {
            caughtException = e;
            System.out.println("Default lock: Failed to open DB after kill: " + e.getMessage());
        }

        if (caughtException != null) {
            // Expected in containers/CI where H2's PID-based stale lock detection fails
            System.out.println(">>> Default lock mode FAILED after process kill (stale .lock.db not detected)");
            System.out.println(">>> This confirms the need for FILE_LOCK=NO or AUTO_SERVER=TRUE");
            assertThat(caughtException.getMessage()).contains("Database may be already in use");
        } else {
            // Expected on Linux with local filesystem — H2 detected stale lock via PID check
            System.out.println(">>> Default lock mode SUCCEEDED (H2 detected stale lock via PID check)");
            System.out.println(">>> In CI/containers, this may fail; FILE_LOCK=NO recommended for portability");
        }
    }

    /**
     * Tests FILE_LOCK=NO after SIGKILL.
     *
     * FILE_LOCK=NO skips both the OS-level lock and the .lock.db file entirely.
     * This is the simplest and most portable fix for the kill-restart scenario:
     * <ul>
     *   <li>No .lock.db file to become stale</li>
     *   <li>No PID check that can fail in containers</li>
     *   <li>No OS-level lock semantics to worry about</li>
     *   <li>Works on all OSes and filesystems</li>
     * </ul>
     *
     * Trade-off: no protection against two processes accidentally opening the same file
     * (silent corruption). Acceptable because:
     * <ul>
     *   <li>H2 is a non-production path (code warns "not suitable for production")</li>
     *   <li>CI test harness guarantees sequential access (kill then restart)</li>
     *   <li>The lock file is advisory only — it doesn't prevent data corruption anyway</li>
     * </ul>
     */
    @Test
    void testFileLockNoAfterKill() throws Exception {
        // Child uses default lock (simulating current production code in H2StateManager)
        String childJdbcUrl = "jdbc:h2:file:" + dbFilePath + ";MODE=PostgreSQL";
        // Recovery uses FILE_LOCK=NO to bypass any stale lock
        String recoveryJdbcUrl = "jdbc:h2:file:" + dbFilePath + ";MODE=PostgreSQL;FILE_LOCK=NO";

        spawnAndKillChildProcess(childJdbcUrl);

        Path mvFile = Path.of(dbFilePath + ".mv.db");
        assertThat(mvFile).exists();

        try (Connection conn = DriverManager.getConnection(recoveryJdbcUrl, "sa", "");
             Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM test_data");
            rs.next();
            int count = rs.getInt(1);
            System.out.println("FILE_LOCK=NO: Successfully opened DB, row count = " + count);
            assertThat(count).isEqualTo(1);

            rs = stmt.executeQuery("SELECT data_value FROM test_data WHERE data_key = 'event_i'");
            rs.next();
            String value = rs.getString(1);
            System.out.println("FILE_LOCK=NO: Recovered data: data_key=event_i, data_value=" + value);
            assertThat(value).isEqualTo("1");
        }
    }

    /**
     * Tests AUTO_SERVER=TRUE after SIGKILL.
     *
     * AUTO_SERVER=TRUE makes H2 start an embedded TCP server so that multiple JVM processes
     * can access the same database file. The first connection starts the server; subsequent
     * connections (even from other JVMs) are transparently routed through TCP.
     *
     * After SIGKILL, the TCP port is released by the OS, so the next process can start a
     * fresh server and open the database. This works, but is heavier than FILE_LOCK=NO:
     * <ul>
     *   <li>Opens a random TCP port (security concern in containers, may conflict with firewall rules)</li>
     *   <li>Adds TCP overhead for all database operations (even single-process usage)</li>
     *   <li>The server port is stored in the .lock.db file; stale port info can cause connection timeouts</li>
     *   <li>Solves concurrent multi-process access, which is unnecessary for sequential kill-restart</li>
     * </ul>
     *
     * AUTO_SERVER=TRUE is the right choice when multiple live processes need to share one
     * H2 file simultaneously. For sequential kill-restart, FILE_LOCK=NO is simpler and safer.
     */
    @Test
    void testAutoServerAfterKill() throws Exception {
        String jdbcUrl = "jdbc:h2:file:" + dbFilePath + ";MODE=PostgreSQL;AUTO_SERVER=TRUE";

        spawnAndKillChildProcess(jdbcUrl);

        Path mvFile = Path.of(dbFilePath + ".mv.db");
        assertThat(mvFile).exists();

        try (Connection conn = DriverManager.getConnection(jdbcUrl, "sa", "");
             Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM test_data");
            rs.next();
            int count = rs.getInt(1);
            System.out.println("AUTO_SERVER=TRUE: Successfully opened DB, row count = " + count);
            assertThat(count).isEqualTo(1);

            rs = stmt.executeQuery("SELECT data_value FROM test_data WHERE data_key = 'event_i'");
            rs.next();
            String value = rs.getString(1);
            System.out.println("AUTO_SERVER=TRUE: Recovered data: data_key=event_i, data_value=" + value);
            assertThat(value).isEqualTo("1");
        }
    }

    /**
     * Spawns a child JVM process that:
     * <ol>
     *   <li>Opens an H2 database at the given JDBC URL</li>
     *   <li>Creates a table and inserts a row</li>
     *   <li>Executes CHECKPOINT SYNC to force data to disk</li>
     *   <li>Prints "READY" to stdout</li>
     *   <li>Waits forever (simulating a long-running process like ansible-rulebook)</li>
     * </ol>
     *
     * The parent then forcibly kills the child (destroyForcibly = SIGKILL on Linux),
     * simulating a crash with no clean shutdown, no H2 SHUTDOWN command, and no
     * JVM shutdown hooks. The .lock.db file is left on disk.
     */
    private void spawnAndKillChildProcess(String jdbcUrl) throws Exception {
        String javaHome = System.getProperty("java.home");
        String classpath = System.getProperty("java.class.path");

        ProcessBuilder pb = new ProcessBuilder(
                javaHome + "/bin/java",
                "-cp", classpath,
                H2ChildProcess.class.getName(),
                jdbcUrl
        );
        pb.redirectErrorStream(true);

        Process child = pb.start();

        // Read stdout until READY signal. Do NOT use try-with-resources here —
        // closing the stream before killing can cause pipe deadlocks if the child
        // has buffered output. Kill first, then let the stream be GC'd.
        InputStream childOutput = child.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(childOutput));
        String line;
        boolean ready = false;
        while ((line = reader.readLine()) != null) {
            System.out.println("[child] " + line);
            if (line.contains("READY")) {
                ready = true;
                break;
            }
            if (line.contains("ERROR")) {
                child.destroyForcibly();
                throw new RuntimeException("Child process failed: " + line);
            }
        }
        assertThat(ready).as("Child process should signal READY").isTrue();

        // Kill FIRST, then the stream is abandoned. destroyForcibly sends SIGKILL
        // which terminates the process immediately — no shutdown hooks, no H2 SHUTDOWN.
        System.out.println("Killing child process (pid=" + child.pid() + ") with destroyForcibly (SIGKILL)...");
        child.destroyForcibly();
        child.waitFor();
        System.out.println("Child process killed, exit code: " + child.exitValue());

        // Brief delay to let OS finalize file handle / lock file cleanup
        Thread.sleep(500);
    }

    /**
     * Standalone main class that runs in a child JVM.
     * Opens H2, writes data, forces checkpoint, signals READY, then blocks forever.
     *
     * The CHECKPOINT SYNC guarantees data is on disk before READY is signaled.
     * In the real {@code H2StateManager}, CHECKPOINT SYNC is not used — H2's WAL
     * (write-ahead log) recovery handles replaying committed transactions on next open.
     * The explicit checkpoint here eliminates any timing window in this short-lived test
     * where SIGKILL arrives milliseconds after the INSERT.
     */
    public static class H2ChildProcess {
        public static void main(String[] args) throws Exception {
            String jdbcUrl = args[0];
            System.out.println("Child opening H2: " + jdbcUrl);

            Connection conn = DriverManager.getConnection(jdbcUrl, "sa", "");
            Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE IF NOT EXISTS test_data (data_key VARCHAR(255), data_value VARCHAR(255))");
            stmt.execute("INSERT INTO test_data (data_key, data_value) VALUES ('event_i', '1')");

            // Force H2 to flush data to disk. CHECKPOINT SYNC forces an immediate MVStore
            // write + fsync. Without this, the INSERT is committed in H2's engine but may
            // only be in the OS page cache when SIGKILL arrives, resulting in
            // "Table not found (this database is empty)" on recovery.
            stmt.execute("CHECKPOINT SYNC");

            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM test_data");
            rs.next();
            System.out.println("Child wrote " + rs.getInt(1) + " row(s), checkpoint done");

            System.out.println("READY");
            System.out.flush();

            // Block forever — parent will SIGKILL us.
            // Connection and lock file intentionally left open to simulate crash.
            Thread.sleep(Long.MAX_VALUE);
        }
    }
}
