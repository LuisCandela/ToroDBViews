package com.toroDBViews.connection;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jooq.Configuration;
import org.jooq.ConnectionProvider;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.RenderNameStyle;
import org.jooq.conf.Settings;
import org.jooq.conf.StatementType;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConfiguration;

import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.db.exceptions.InvalidDatabaseException;
import com.torodb.torod.db.postgresql.meta.TorodbMeta;
import com.torodb.torod.db.sql.AbstractSqlDbWrapper.MyConnectionProvider;

public class PostgreSQLConnection {

	private final AtomicBoolean isInitialized = new AtomicBoolean(false);
	private Connection c;
	private DSLContext dsl;
	private ConfigureConnection configure;
	TorodbMeta meta;
	private String databaseName;

	public PostgreSQLConnection(ConfigureConnection configure) {
		this.configure = configure;
		this.databaseName = configure.getDbName();
	}

	public void initialize() throws ImplementationDbException, ClassNotFoundException {
		if (isInitialized()) {
			throw new IllegalStateException("The db-wrapper is already initialized");
		}

		try {
			createConnection();
			createDsl(c);
			meta = new TorodbMeta(databaseName, dsl);
			c.commit();

			isInitialized.set(true);
		} catch (IOException ex) {
			// TODO: Change exception
			throw new RuntimeException(ex);
		} catch (SQLException ex) {
			// TODO: Change exception
			throw new RuntimeException(ex);
		} catch (DataAccessException ex) {
			// TODO: Change exception
			throw new RuntimeException(ex);
		} catch (InvalidDatabaseException ex) {
			// TODO: Change exception
			throw new RuntimeException(ex);
		} finally {

			closeConection();
		}

	}

	public Connection openConection() throws ClassNotFoundException, ImplementationDbException {

		try {
			createConnection();
			createDsl(c);

		} catch (SQLException ex) {
			// TODO: Change exception
			throw new RuntimeException(ex);
		} catch (DataAccessException ex) {
			// TODO: Change exception
			throw new RuntimeException(ex);
		}
		return c;
	}

	public void closeConection() {
		try {
			if (c != null) {

				c.close();
			}
		} catch (SQLException ex) {
		}

	}

	private void checkDbSupported(Connection conn) throws SQLException, ImplementationDbException {
		int major = conn.getMetaData().getDatabaseMajorVersion();
		int minor = conn.getMetaData().getDatabaseMinorVersion();

		if (!(major > configure.getDbSupportMajor()
				|| (major == configure.getDbSupportMajor() && minor >= configure.getDbSupportMinor()))) {
			throw new ImplementationDbException(true,
					"ToroDB requires PostgreSQL version " + configure.getDbSupportMajor() + "."
							+ configure.getDbSupportMinor() + " or higher! Detected " + major + "." + minor);
		}
	}

	private void createConnection() throws ClassNotFoundException, ImplementationDbException, SQLException {
		Class.forName("org.postgresql.Driver");
		c = DriverManager.getConnection(configure.getUrl() + databaseName, configure.getUsername(),
				configure.getPassword());
		checkDbSupported(c);
		c.setAutoCommit(false);

	}

	private void createDsl(Connection c) {
		dsl = DSL.using(getJooqConfiguration(new MyConnectionProvider(c)));

	}

	private Configuration getJooqConfiguration(ConnectionProvider cp) {
		Settings settings = new Settings();
		settings.withRenderNameStyle(RenderNameStyle.QUOTED);
		settings.setStatementType(StatementType.STATIC_STATEMENT);

		return new DefaultConfiguration().set(cp).set(SQLDialect.POSTGRES).set(settings);
	}

	private boolean isInitialized() {
		return isInitialized.get();
	}

	public TorodbMeta getMeta() {
		return meta;
	}

	public DSLContext getDsl() {
		return dsl;
	}
}
