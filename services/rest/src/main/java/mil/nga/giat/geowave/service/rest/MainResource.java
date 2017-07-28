package mil.nga.giat.geowave.service.rest;

import java.util.ArrayList;

import javax.servlet.ServletContext;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainResource extends
		ServerResource
{
	private static final Logger LOGGER = LoggerFactory.getLogger(MainResource.class);

	/**
	 * This is the main resource (essentially index.html) it displays the user's
	 * API Key and the list of mapped commands, it also displays the user's
	 * apiKey if the GeoWaveApiKeyFilter and GeoWaveApiKeySetterFilter
	 */

	@Get("html")
	public String listResources() {
		String output = "";
		try {
			final StringBuilder routeStringBuilder = new StringBuilder();
			final ServletContext servletContext = (ServletContext) getContext().getAttributes().get(
					"org.restlet.ext.servlet.ServletContext");
			final String userName = (String) servletContext.getAttribute("userName");
			final String apiKey = (String) servletContext.getAttribute("apiKey");
			final ArrayList<RestRoute> availableRoutes = (ArrayList<RestRoute>) getContext().getAttributes().get(
					"availableRoutes");

			routeStringBuilder.append("Available Routes:<br>");

			for (final RestRoute route : availableRoutes) {
				routeStringBuilder.append(route.getPath() + " --> " + route.getOperation() + "<br>");
			}

			if (userName != null && !userName.equals("")) {
				output = "<b>Welcome " + userName + "!</b><br><b>API key:</b> " + apiKey + "<br><br>"
						+ routeStringBuilder.toString();
			}
			else {
				output = routeStringBuilder.toString();
			}
		}
		catch (Exception e) {
			LOGGER.error(
					"Error listing resources",
					e);
		}
		return output;
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
