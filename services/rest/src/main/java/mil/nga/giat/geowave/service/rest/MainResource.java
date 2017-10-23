package mil.nga.giat.geowave.service.rest;

import java.util.ArrayList;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

public class MainResource extends
		ServerResource
{

	/**
	 * This is the main resource (essentially index.html) it displays the user's
	 * API Key and the list of mapped commands
	 */

	@Get("html")
	public String listResources() {

		final StringBuilder routeStringBuilder = new StringBuilder();

		final ArrayList<RestRoute> availableRoutes = (ArrayList<RestRoute>) getContext().getAttributes().get(
				"availableRoutes");
		final ArrayList<String> unavailableCommands = (ArrayList<String>) getContext().getAttributes().get(
				"unavailableCommands");

		routeStringBuilder.append(
				"Available Routes:<br>");

		for (final RestRoute route : availableRoutes) {
			routeStringBuilder.append(
					route.getPath() + " --> " + route.getOperation() + "<br>");
		}

		routeStringBuilder.append(
				"<br><br><span style='color:blue'>Unavailable Routes:</span><br>");
		for (final String command : unavailableCommands) {
			routeStringBuilder.append(
					"<span style='color:blue'>" + command + "</span><br>");
		}

		return routeStringBuilder.toString();
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
