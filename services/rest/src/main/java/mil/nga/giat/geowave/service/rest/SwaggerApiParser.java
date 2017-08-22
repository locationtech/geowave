package mil.nga.giat.geowave.service.rest;

import java.io.FileOutputStream;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;

public class SwaggerApiParser
{
	private static final Logger LOGGER = LoggerFactory.getLogger(SwaggerApiParser.class);
	/**
	 * Reads RestRoute(s) and operations and parses class fields for particular
	 * annotations ( @Parameter and @ParametersDelegate from JCommander) The
	 * parsed data is then used to build up JSON objects that can be written to
	 * file and used by Swagger for API documentation and generation
	 */

	private final JsonObject routesJson;
	private final String swaggerHeader;

	public SwaggerApiParser(
			final String host,
			final String apiVersion,
			final String apiTitle,
			final String apiDescription ) {
		this.routesJson = new JsonObject();
		this.swaggerHeader = "{\"swagger\": \"2.0\"," + "\"info\": {" + "\"version\": \"" + apiVersion + "\","
				+ "\"title\": \"" + apiTitle + "\"," + "\"description\": \"" + apiDescription + "\","
				+ "\"termsOfService\": \"http://localhost:5152/\"," + "\"contact\": {" + "\"name\": \"GeoWave Team\""
				+ "}," + "\"license\": {" + "\"name\": \"MIT\"" + "}" + "}," + "\"host\": \"" + host + "\","
				+ "\"basePath\": \"/\"," + "\"schemes\": [" + "\"http\"" + "]," + "\"consumes\": ["
				+ "\"application/json\"" + "]," + "\"produces\": [" + "\"application/json\"" + "]," + "\"paths\":";
	}

	public void addRoute(
			final RestRoute route ) {
		final ServiceEnabledCommand<?> instance = route.getOperation();
		// iterate over routes and paths here
		LOGGER.info("OPERATION: " + route.getPath() + " : " + instance.getClass().getName());
		final SwaggerOperationParser parser = new SwaggerOperationParser<>(
				instance);
		final JsonObject op_json = parser.getJsonObject();

		final JsonObject method_json = new JsonObject();
		final String method = instance.getMethod().toString();

		final JsonArray tags_json = new JsonArray();
		final String[] path_toks = route.getPath().split(
				"/");
		final JsonPrimitive tag = new JsonPrimitive(
				path_toks[1]);
		tags_json.add(tag);

		op_json.add(
				"tags",
				tags_json);

		method_json.add(
				method.toLowerCase(),
				op_json);

		routesJson.add(
				"/" + route.getPath(),
				method_json);
	}

	public boolean serializeSwaggerJson(
			final String filename ) {
		Writer writer = null;
		try {
			writer = new OutputStreamWriter(
						new FileOutputStream(
							filename),
						"UTF-8");
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to write swagger json",
					e);
		}
		if (writer == null) return false;
		
		final Gson gson = new GsonBuilder().create();

			try {
				writer.write(swaggerHeader);
				gson.toJson(
						routesJson,
						writer);
				writer.write("}");
				writer.close();
			}
			catch (final IOException e1) {
				e1.printStackTrace();
			}
		
		return true;
	}
}
