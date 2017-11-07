package mil.nga.giat.geowave.service.rest;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
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
	private final String fileUpload;

	public SwaggerApiParser(
			final String host,
			final String path,
			final String apiVersion,
			final String apiTitle,
			final String apiDescription ) {
		routesJson = new JsonObject();
		swaggerHeader = "{\"swagger\": \"2.0\"," + "\"info\": {" + "\"version\": \"" + apiVersion + "\","
				+ "\"title\": \"" + apiTitle + "\"," + "\"description\": \"" + apiDescription + "\","
				+ "\"termsOfService\": \"https://github.com/locationtech/geowave\"," + "\"contact\": {"
				+ "\"name\": \"GeoWave Team\"" + "}," + "\"license\": {" + "\"name\": \"Apache2\"" + "}" + "},"
				+ "\"host\": \"" + host + "\"," + "\"basePath\": \"" + path + "\"," + "\"schemes\": [" + "\"http\""
				+ "]," + "\"consumes\": [" + "\"application/json\",\"multipart/form-data\"" + "]," + "\"produces\": ["
				+ "\"application/json\"" + "]," + "\"paths\":";

		fileUpload = ",\"/v0/fileupload\": {\"post\":{\"operationId\": \"fileupload\",\"consumes\": [\"multipart/form-data\""
				+ "],"
				+ "\"description\": \"Get the version of GeoWave running on the instance of a remote datastore\",\"parameters\": [{\"name\": \"body\",\"description\": \"file detail\",\"required\": false,\"type\": \"file\",\"paramType\": \"body\",\"in\": \"formData\",\"allowMultiple\": false	}],	\"responses\": {		\"200\": {	\"description\": \"success\"	},\"404\": {	\"description\": \"route not found\"},\"500\": {	\"description\": \"invalid or null parameter\"}	},\"tags\": [\"fileupload\"]}}}";
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
		if (writer == null) {
			return false;
		}

		final Gson gson = new GsonBuilder().create();

		try {
			writer.write(swaggerHeader);
			final StringWriter strWriter = new StringWriter();
			gson.toJson(
					routesJson,
					strWriter);
			// TODO make this a bit cleaner, for now just remove the closing
			// brace within the routes so that the file upload service can be
			// appended and then re-add the closing brace
			strWriter.getBuffer().deleteCharAt(
					strWriter.getBuffer().length() - 1);
			writer.write(strWriter.getBuffer().toString());
			writer.write(fileUpload);
			writer.write('}');
			writer.close();
		}
		catch (final IOException e1) {
			e1.printStackTrace();
		}
		finally {
			safeClose(writer);
		}

		return true;
	}

	public static void safeClose(
			Writer writer ) {
		if (writer != null) {
			try {
				writer.close();
			}
			catch (IOException e) {
				LOGGER.warn(
						"Unable to close Writer",
						e);
			}
		}
	}
}
