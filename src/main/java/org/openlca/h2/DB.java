package org.openlca.h2;

import com.zaxxer.hikari.HikariDataSource;
import org.eclipse.persistence.jpa.PersistenceProvider;
import org.h2.Driver;
import org.openlca.core.database.DatabaseException;
import org.openlca.core.database.DbUtils;
import org.openlca.core.database.IDatabase;
import org.openlca.core.database.Notifiable;
import org.openlca.core.database.internal.ScriptRunner;
import org.openlca.util.Dirs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManagerFactory;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class DB extends Notifiable implements IDatabase {

	private static final AtomicInteger memInstances = new AtomicInteger(0);

	private final Logger log = LoggerFactory.getLogger(getClass());
	private final AtomicBoolean closed = new AtomicBoolean(false);
	private final String url;
	private final String name;
	private HikariDataSource connectionPool;
	private EntityManagerFactory entityFactory;
	private File fileStorage;


	public static DB inMempory() {
		return new DB();
	}

	private DB() {
		registerDriver();
		name = "memdb" + memInstances.incrementAndGet();
		url = "jdbc:h2:mem:" + name + ";MODE=MySQL";
		createNew(url);
		connect();
	}

	private void createNew(String url) {
		log.info("create new H database {}", url);
		try {
			fileStorage = Files.createTempDirectory("_olca_h2").toFile();
			log.info("files will be stored in {}; and deleted on close");
			Connection con = DriverManager.getConnection(url);
			con.close();
			ScriptRunner runner = new ScriptRunner(this);
			InputStream ddl = getClass().getResourceAsStream("schema.sql");
			runner.run(ddl, "utf-8");
			ddl.close();
		} catch (Exception e) {
			log.error("failed to create database", e);
			throw new DatabaseException("Failed to create database", e);
		}
	}

	private void connect() {
		log.trace("connect to database: {}", url);
		Map<Object, Object> map = new HashMap<>();
		map.put("javax.persistence.jdbc.url", url);
		map.put("javax.persistence.jdbc.driver",
				"org.apache.derby.jdbc.EmbeddedDriver");
		map.put("eclipselink.classloader", getClass().getClassLoader());
		map.put("eclipselink.target-database", "Derby");
		entityFactory = new PersistenceProvider().createEntityManagerFactory(
				"openLCA", map);
		initConnectionPool();
	}

	private void initConnectionPool() {
		try {
			connectionPool = new HikariDataSource();
			connectionPool.setJdbcUrl(url);
		} catch (Exception e) {
			log.error("failed to initialize connection pool", e);
			throw new DatabaseException("Could not create a connection", e);
		}
	}

	@Override
	public Connection createConnection() {
		log.trace("create connection: {}", url);
		try {
			if (connectionPool != null) {
				Connection con = connectionPool.getConnection();
				con.setAutoCommit(false);
				return con;
			} else {
				log.warn("no connection pool set up for {}", url);
				return DriverManager.getConnection(url);
			}
		} catch (Exception e) {
			log.error("Failed to create database connection", e);
			return null;
		}
	}

	private void registerDriver() {
		try {
			DriverManager.registerDriver(new Driver());
		} catch (Exception e) {
			throw new RuntimeException("Could not register H2 driver", e);
		}
	}

	@Override
	public EntityManagerFactory getEntityFactory() {
		return entityFactory;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public int getVersion() {
		return DbUtils.getVersion(this);
	}

	@Override
	public void close() {
		if (closed.get())
			return;
		try {
			if (entityFactory != null && entityFactory.isOpen()) {
				entityFactory.close();
				entityFactory = null;
			}
			if (connectionPool != null && !connectionPool.isClosed()) {
				connectionPool.close();
				connectionPool = null;
			}
			Connection con = DriverManager.getConnection(url);
			con.createStatement().execute("SHUTDOWN");
			con.close();
			System.gc();
			Dirs.delete(fileStorage.getPath());
		} catch (Exception e) {
			log.error("failed to close H2 database", e);
		}
	}

	@Override
	public File getFileStorageLocation() {
		return fileStorage;
	}
}
