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
import java.sql.Statement;
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
	private final HikariDataSource pool;
	private EntityManagerFactory entityFactory;
	private File fileStorage;

	public static DB empty() {
		DB db = new DB();
		db.createNew();
		db.connectJPA();
		return db;
	}

	public static DB fromDump(String file) {
		DB db = new DB();
		try (Connection con = db.createConnection()) {
			Statement stmt = con.createStatement();
			stmt.execute("RUNSCRIPT FROM '" + file + "' COMPRESSION GZIP");
			stmt.close();
			db.connectJPA();
			return db;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private DB() {
		registerDriver();
		name = "memdb" + memInstances.incrementAndGet();
		url = "jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE";
		pool = new HikariDataSource();
		pool.setJdbcUrl(url);
		pool.setUsername("sa");
		pool.setPassword("");
	}

	private void createNew() {
		log.info("create new H database {}", url);
		try {
			fileStorage = Files.createTempDirectory("_olca_h2").toFile();
			log.info("files will be stored in {}; and deleted on close");
			ScriptRunner runner = new ScriptRunner(this);
			InputStream ddl = getClass().getResourceAsStream("schema.sql");
			runner.run(ddl, "utf-8");
			ddl.close();
		} catch (Exception e) {
			log.error("failed to create database", e);
			throw new DatabaseException("Failed to create database", e);
		}
	}

	private void connectJPA() {
		log.trace("connect to database: {}", url);
		Map<Object, Object> map = new HashMap<>();
		map.put("javax.persistence.jtaDataSource", pool);
		map.put("eclipselink.classloader", getClass().getClassLoader());
		map.put("eclipselink.target-database", "HSQL");
		map.put("transaction-type", "JTA");
		entityFactory = new PersistenceProvider().createEntityManagerFactory(
				"olcaH2", map);
	}

	@Override
	public Connection createConnection() {
		log.trace("create connection: {}", url);
		try {
			Connection con = pool.getConnection();
			con.setAutoCommit(false);
			return con;
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

	public void dump(String file) {
		try (Connection con = createConnection()) {
			Statement stmt = con.createStatement();
			stmt.execute("SCRIPT TO '" + file + "' COMPRESSION GZIP");
			stmt.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
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
			if (pool != null && !pool.isClosed()) {
				Connection con = createConnection();
				Statement stmt = con.createStatement();
				stmt.execute("SHUTDOWN");
				pool.close();
			}
			System.gc();
			if (fileStorage != null)
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
