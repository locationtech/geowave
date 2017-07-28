package mil.nga.giat.geowave.service.rest;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.service.rest.RestRoute;

public class SwaggerApiParser
{
	/**
	 * Reads RestRoute(s) and operations and parses class fields for particular
	 * annotations ( @Parameter and @ParametersDelegate from JCommander) The
	 * parsed data is then used to build up JSON objects that can be written to
	 * file and used by Swagger for API documentation and generation
	 */

	private JsonObject routesJson;
	private String swaggerHeader;

	public SwaggerApiParser(
			String apiVersion,
			String apiTitle,
			String apiDescription ) {
		this.routesJson = new JsonObject();
		this.swaggerHeader = "{\"swagger\": \"2.0\"," + "\"info\": {" + "\"version\": \"" + apiVersion + "\","
				+ "\"title\": \"" + apiTitle + "\"," + "\"description\": \"" + apiDescription + "\","
				+ "\"termsOfService\": \"http://localhost:5152/\"," + "\"contact\": {" + "\"name\": \"GeoWave Team\""
				+ "}," + "\"license\": {" + "\"name\": \"MIT\"" + "}" + "}," + "\"host\": \"localhost:5152\","
				+ "\"basePath\": \"/\"," + "\"schemes\": [" + "\"http\"" + "]," + "\"consumes\": ["
				+ "\"application/json\"" + "]," + "\"produces\": [" + "\"application/json\"" + "]," + "\"paths\":";
	}

	public void AddRoute(
			RestRoute route ) {
		Class<? extends DefaultOperation<?>> opClass = ((Class<? extends DefaultOperation<?>>) route.getOperation());
		// iterate over routes and paths here
		SwaggerOperationParser parser = null;
		try {
			System.out.println("OPERATION: " + route.getPath() + " : " + opClass.getName());

			parser = new SwaggerOperationParser<>(
					opClass.newInstance());
			JsonObject op_json = ((SwaggerOperationParser) parser).GetJsonObject();

			JsonObject method_json = new JsonObject();
			String method = route.getOperation().getAnnotation(
					GeowaveOperation.class).restEnabled().toString();

			JsonArray tags_json = new JsonArray();
			String[] path_toks = route.getPath().split(
					"/");
			JsonPrimitive tag = new JsonPrimitive(
					path_toks[path_toks.length - 2]);
			tags_json.add(tag);

			op_json.add(
					"tags",
					tags_json);

			method_json.add(
					method.toLowerCase(),
					op_json);

			this.routesJson.add(
					"/" + route.getPath(),
					method_json);
		}
		catch (InstantiationException | IllegalAccessException e) {
			System.out.println("Exception while instantiating the geowave operation server resource.");
		}
	}

	public void SerializeSwaggerJson(
			String filename ) {
		Writer writer = null;
		try {
			writer = new FileWriter(
					filename);
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		Gson gson = new GsonBuilder().create();

		try {
			writer.write(this.swaggerHeader);
			gson.toJson(
					this.routesJson,
					writer);
			writer.write("}");
			writer.close();
		}
		catch (IOException e1) {
			e1.printStackTrace();
		}
	}

}
