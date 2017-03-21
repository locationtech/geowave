package mil.nga.giat.geowave.service.rest;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.shaded.restlet.Context;
import org.restlet.ext.json.JsonRepresentation;
import org.shaded.restlet.resource.ResourceException;
import org.shaded.restlet.data.Form;
import org.shaded.restlet.Response;
import org.shaded.restlet.Client;
import org.shaded.restlet.Request;
import org.shaded.restlet.data.MediaType;
import org.shaded.restlet.data.Method;
import org.shaded.restlet.data.Protocol;
import org.shaded.restlet.data.Status;
import org.shaded.restlet.representation.Representation;
import org.shaded.restlet.resource.ClientResource;
import org.shaded.restlet.resource.Resource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

public class RestServerTest
{
	@BeforeClass
	public static void runServer() {
		RestServer.main(new String[] {});
	}

	public Response request(
			String path,
			Method method ) {

		Client client = new Client(
				Protocol.HTTP);
		Request request = new Request(
				method,
				"http://localhost:5152/" + path);

		return client.handle(request);
	}

	// Tests geowave/config/list
	@Test
	public void geowave_config_list()
			throws IOException,
			ParseException {

		// OperationParams params = new ManualOperationParams();
		// params.getContext().put(
		// ConfigOptions.PROPERTIES_FILE_CONTEXT,
		// ConfigOptions.getDefaultPropertyFile());
		// File f = (File) params.getContext().get(
		// ConfigOptions.PROPERTIES_FILE_CONTEXT);
		// Properties p = ConfigOptions.loadProperties(
		// f,
		// null);
		//
		// String key = "name";
		// String value = "value";
		//
		// assert p != null;
		//
		// p.setProperty(
		// key,
		// value);
		// ConfigOptions.writeProperties(
		// f,
		// p);
		//
		// Response response = request(
		// "geowave/config/list",
		// Method.GET);
		//
		// assertTrue(
		// "Status is 200",
		// response.getStatus().getCode() == 200);
		//
		// assertTrue(
		// "Has a body",
		// response.isEntityAvailable());
		//
		// assertTrue(
		// "Body is JSON",
		// response.getEntity().getMediaType().equals(
		// MediaType.APPLICATION_JSON));
		//
		// String text = response.getEntity().getText();
		//
		// JSONParser parser = new JSONParser();
		// JSONObject obj = (JSONObject) parser.parse(text);
		//
		// assertTrue(
		// "JSON can be parsed",
		// obj != null);
		//
		// String name = (String) obj.get("name");
		//
		// assertTrue(
		// "List contains 'name'",
		// name != null);
		// assertTrue(
		// "'name' is 'value'",
		// name.equals("value"));

	}

	// Tests geowave/config/set
	@Test
	public void geowave_config_set()
			throws ResourceException,
			IOException {

	}

	// Tests geowave/config/addstore, cpstore, rmstore
	@Test
	public void geowave_config_store()
			throws ResourceException,
			IOException {

		// create a new store named "store1", with type "hbase"
		ClientResource resourceAdd = new ClientResource(
				"http://localhost:5152/geowave/config/addstore");
		Form formAdd = new Form();
		formAdd.add(
				"name",
				"store1");
		formAdd.add(
				"storetype",
				"hbase");
		formAdd.add(
				"default",
				"true");
		resourceAdd.post(
				formAdd).write(
				System.out);

		// create a new store named "store2"
		ClientResource resourceCp = new ClientResource(
				"http://localhost:5152/geowave/config/cpstore");
		Form formCp = new Form();
		formCp.add(
				"name",
				"store1");
		formCp.add(
				"newname",
				"store2");
		formCp.add(
				"default",
				"true");
		resourceCp.post(
				formCp).write(
				System.out);

		// remove the store named "store1" and "store2"
		ClientResource resourceRm = new ClientResource(
				"http://localhost:5152/geowave/config/rmstore");
		Form formRm = new Form();
		formRm.add(
				"name",
				"store1");
		resourceRm.post(
				formRm).write(
				System.out);
		formRm.remove(0);
		formRm.add(
				"name",
				"store2");
		resourceRm.post(
				formRm).write(
				System.out);
	}

	// Tests geowave/config/addindex, cpindex, rmindex
	@Test
	public void geowave_config_index()
			throws ResourceException,
			IOException {

	}

	// Tests geowave/config/addindexgrp, rmindexgrp
	@Test
	public void geowave_config_indexgrp()
			throws ResourceException,
			IOException {

	}

}
