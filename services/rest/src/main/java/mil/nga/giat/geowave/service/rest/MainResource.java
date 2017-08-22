package mil.nga.giat.geowave.service.rest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.UUID;
import java.util.logging.Level;

import org.geoserver.ows.Request;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

public class MainResource extends
		ServerResource
{

	/**
	 * This is the main resource (essentially index.html) it displays the user's
	 * API Key and the list of mapped commands
	 */

	@Get("html")
	public String listResources() {

		final SecurityContext context = SecurityContextHolder.getContext();
		final String username = context.getAuthentication().getName();

		// key will be appended below
		String userKey = "";

		final String dbUrl = (String) getContext().getAttributes().get(
				"databaseUrl");

		try (Connection conn = DriverManager.getConnection(dbUrl)) {
			if (conn != null) {

				final String sql_query = "SELECT * FROM api_keys WHERE username=?;";
				PreparedStatement query_stmnt = conn.prepareStatement(sql_query);
				query_stmnt.setString(
						1,
						username);
				ResultSet rs = query_stmnt.executeQuery();
				// There is no existing row, so we should generate a key for
				// this user and add it to
				// the table
				if (!rs.next()) {

					// close resources we are done with
					rs.close();
					query_stmnt.close();

					// generate new api key
					final UUID apiKey = UUID.randomUUID();
					userKey = apiKey.toString();

					// SQL statement for inserting a new user/api key
					final String sql = "INSERT INTO api_keys (apiKey, username)\n" + "VALUES(?, ?);";
					getContext().getLogger().info("Inserting a new api key and user.");
					PreparedStatement stmnt = conn.prepareStatement(sql);
					stmnt.setString(
							1,
							apiKey.toString());
					stmnt.setString(
							2,
							username);
					stmnt.executeUpdate();
					stmnt.close();
				}
				else {
					final String apiKeyStr = rs.getString("apiKey");
					userKey = apiKeyStr;
					// close resources we are done with
					rs.close();
					query_stmnt.close();
				}
				conn.close();
			}

		}
		catch (SQLException e) {
			getContext().getLogger().log(Level.SEVERE, e.getMessage());
		}

		final ArrayList<RestRoute> availableRoutes = (ArrayList<RestRoute>) getContext().getAttributes().get(
				"availableRoutes");
		final ArrayList<String> unavailableCommands = (ArrayList<String>) getContext().getAttributes().get(
				"unavailableCommands");

		final StringBuilder routeStringBuilder = new StringBuilder(
				"Available Routes:<br>");

		for (final RestRoute route : availableRoutes) {
			routeStringBuilder.append(route.getPath() + " --> " + route.getOperation() + "<br>");
		}

		routeStringBuilder.append("<br><br><span style='color:blue'>Unavailable Routes:</span><br>");
		for (final String command : unavailableCommands) {
			routeStringBuilder.append("<span style='color:blue'>" + command + "</span><br>");
		}

		return "<b>Welcome " + username + "!</b><br><b>API key:</b> " + userKey + "<br><br>"
				+ routeStringBuilder.toString();
	}

	/**
	 * A simple ServerResource to show if the route's operation does not extend
	 * ServerResource
	 */
	public static class NonResourceCommand extends
			ServerResource
	{
		@Override
		@Get("html")
		public String toString() {
			return "The route exists, but the command does not extend ServerResource";
		}
	}
}
