package mil.nga.giat.geowave.service.rest.security;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import javax.servlet.ServletContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

public class GeoWaveSQLiteApiKeyDB extends
		GeoWaveBaseApiKeyDB
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveSQLiteApiKeyDB.class);
	/**
	 * An SQLite api-key database implementation.
	 */

	private String dbFileName;
	private String dbPath;

	public GeoWaveSQLiteApiKeyDB() {}

	public GeoWaveSQLiteApiKeyDB(
			final String dbFileName ) {
		this.dbFileName = dbFileName;
	}

	@Override
	public void initApiKeyDatabase() {
		String url = "jdbc:sqlite:" + dbPath + dbFileName;

		try (Connection conn = DriverManager.getConnection(url)) {
			// SQL statement for creating a new table
			String sql = "CREATE TABLE IF NOT EXISTS api_keys (\n" + "	id integer PRIMARY KEY,\n"
					+ "	apiKey blob NOT NULL,\n" + "	username text NOT NULL\n" + ");";

			try (Statement stmnt = conn.createStatement()) {
				stmnt.execute(sql);
			}
		}
		catch (SQLException e) {
			LOGGER.error(
					"Error SQLException: ",
					e.getMessage());
		}
	}

	@Override
	public void setServletContext(
			ServletContext servletContext ) {
		super.setServletContext(servletContext);
		dbPath = servletContext.getRealPath("/");
		initApiKeyDatabase();
	}

	@Override
	public boolean hasKey(
			String apiKey ) {
		final String dbUrl = getDbUrl();
		boolean found = false;
		try (Connection conn = DriverManager.getConnection(dbUrl)) {
			final String sql_query = "SELECT * FROM api_keys WHERE apiKey=?;";
			try (PreparedStatement query_stmnt = conn.prepareStatement(sql_query)) {
				// HP Fortify
				// "Authorization Bypass Through User-Controlled SQL Primary Key"
				// false positive
				// While the actor is passing a value that is used as a primary
				// key look-up, the results
				// of the statement are never accessible by the actor.
				query_stmnt.setString(
						1,
						apiKey);
				try (ResultSet rs = query_stmnt.executeQuery()) {
					// If there is an existing row, the apiKey is valid
					if (rs.next()) {
						found = true;
					}
				}
			}
		}
		catch (SQLException e) {
			LOGGER.error(
					"Error SQLException: ",
					e.getMessage());
			return false;
		}
		return found;
	}

	@Override
	public String getCurrentUserAndKey() {
		final SecurityContext context = SecurityContextHolder.getContext();
		if (context != null) {
			final String username = context.getAuthentication().getName();
			// key will be appended below
			String userKey = "";
			if (username != null) {
				final String dbUrl = getDbUrl();

				// look up the api key from the db
				try (Connection conn = DriverManager.getConnection(dbUrl)) {

					final String sql_query = "SELECT * FROM api_keys WHERE username=?;";
					try (PreparedStatement query_stmnt = conn.prepareStatement(sql_query)) {
						query_stmnt.setString(
								1,
								username);
						try (ResultSet rs = query_stmnt.executeQuery()) {

							// There is no existing row, so we should generate a
							// key
							// for this user and add it to the table
							if (!rs.next()) {

								// generate new api key
								final UUID apiKey = UUID.randomUUID();
								userKey = username + ":" + apiKey.toString();

								// SQL statement for inserting a new user/api
								// key
								final String sql = "INSERT INTO api_keys (apiKey, username)\n" + "VALUES(?, ?);";
								LOGGER.info("Inserting a new api key and user.");

								try (PreparedStatement stmnt = conn.prepareStatement(sql)) {
									stmnt.setString(
											1,
											apiKey.toString());
									stmnt.setString(
											2,
											username);
									stmnt.executeUpdate();
								}
							}
							else {
								final String apiKeyStr = rs.getString("apiKey");
								userKey = username + ":" + apiKeyStr;
							}
						}
					}
				}
				catch (SQLException e) {
					LOGGER.error(
							"Error SQLException: ",
							e.getMessage());
				}
			}
			return userKey;
		}
		return "";
	}

	private String getDbUrl() {
		return "jdbc:sqlite:" + dbPath + dbFileName;
	}
}
