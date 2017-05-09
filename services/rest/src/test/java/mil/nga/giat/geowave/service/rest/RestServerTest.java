package mil.nga.giat.geowave.service.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.shaded.restlet.Context;
import org.shaded.restlet.data.ChallengeScheme;
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
import org.shaded.restlet.representation.FileRepresentation;
import org.shaded.restlet.representation.Representation;
import org.shaded.restlet.resource.ClientResource;
import org.shaded.restlet.resource.Resource;
import org.shaded.restlet.ext.html.FormDataSet;
import org.shaded.restlet.ext.html.FormData;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

public class RestServerTest
{
	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@BeforeClass
	public static void runServer() {
		// Ensure the config directory exists.
		File configDirectory = ConfigOptions.getDefaultPropertyPath();
		if (!configDirectory.exists() && !configDirectory.mkdir()) throw new RuntimeException(
				"Unable to create directory " + configDirectory);
		RestServer.main(new String[] {});

	}

	// Tests geowave/config/set and list
	@Test
	public void geowave_config_set_list()
			throws ResourceException,
			IOException,
			ParseException {

		File configFile = tempFolder.newFile("test_config");

		// create a new store named "store1", with type "memory"
		ClientResource resourceAdd = new ClientResource(
				"http://localhost:5152/geowave/config/addstore");
		resourceAdd.setChallengeResponse(
				ChallengeScheme.HTTP_BASIC,
				"admin",
				"password");
		Form formAdd = new Form();
		formAdd.add(
				"name",
				"store1");
		formAdd.add(
				"storeType",
				"memory");
		formAdd.add(
				"default",
				"false");
		formAdd.add(
				"config_file",
				configFile.getAbsolutePath());
		resourceAdd.post(
				formAdd).write(
				System.out);

		// set key=store1, value=store2
		ClientResource resourceSet = new ClientResource(
				"http://localhost:5152/geowave/config/set");
		resourceSet.setChallengeResponse(
				ChallengeScheme.HTTP_BASIC,
				"admin",
				"password");
		Form formSet = new Form();
		formSet.add(
				"key",
				"store1");
		formSet.add(
				"value",
				"store2");
		formSet.add(
				"config_file",
				configFile.getAbsolutePath());
		resourceSet.post(
				formSet).write(
				System.out);

		// testing list
		ClientResource resourceList = new ClientResource(
				"http://localhost:5152/geowave/config/list");
		resourceList.setChallengeResponse(
				ChallengeScheme.HTTP_BASIC,
				"admin",
				"password");
		resourceList.addQueryParameter(
				"config_file",
				configFile.getAbsolutePath());
		String text = resourceList.get(
				MediaType.APPLICATION_JSON).getText();

		JSONParser parser = new JSONParser();
		JSONObject obj = (JSONObject) parser.parse(text);
		String name = (String) obj.get("store1");
		assertTrue(
				"'name' is 'value'",
				name.equals("store2"));

		// remove the store named "store1"
		ClientResource resourceRm = new ClientResource(
				"http://localhost:5152/geowave/config/rmstore");
		resourceRm.setChallengeResponse(
				ChallengeScheme.HTTP_BASIC,
				"admin",
				"password");
		Form formRm = new Form();
		formRm.add(
				"name",
				"store1");
		formRm.add(
				"config_file",
				configFile.getAbsolutePath());
		resourceRm.post(
				formRm).write(
				System.out);
	}

	// Tests geowave/config/addstore, cpstore, rmstore
	@Test
	public void geowave_config_store()
			throws ResourceException,
			IOException,
			ParseException {

		File configFile = tempFolder.newFile("test_config");

		// create a new store named "store1", with type "memory"
		ClientResource resourceAdd = new ClientResource(
				"http://localhost:5152/geowave/config/addstore");
		resourceAdd.setChallengeResponse(
				ChallengeScheme.HTTP_BASIC,
				"admin",
				"password");
		Form formAdd = new Form();
		formAdd.add(
				"name",
				"store1");
		formAdd.add(
				"storeType",
				"memory");
		formAdd.add(
				"default",
				"true");
		formAdd.add(
				"config_file",
				configFile.getAbsolutePath());
		resourceAdd.post(
				formAdd).write(
				System.out);

		// create a new store named "store2"
		ClientResource resourceCp = new ClientResource(
				"http://localhost:5152/geowave/config/cpstore");
		resourceCp.setChallengeResponse(
				ChallengeScheme.HTTP_BASIC,
				"admin",
				"password");
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
		formCp.add(
				"config_file",
				configFile.getAbsolutePath());
		resourceCp.post(
				formCp).write(
				System.out);

		// remove the store named "store1" and "store2"
		ClientResource resourceRm = new ClientResource(
				"http://localhost:5152/geowave/config/rmstore");
		resourceRm.setChallengeResponse(
				ChallengeScheme.HTTP_BASIC,
				"admin",
				"password");
		Form formRm = new Form();
		formRm.add(
				"name",
				"store1");
		formRm.add(
				"config_file",
				configFile.getAbsolutePath());
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

		// use list to test if removed successfully
		ClientResource resourceList = new ClientResource(
				"http://localhost:5152/geowave/config/list");
		resourceList.setChallengeResponse(
				ChallengeScheme.HTTP_BASIC,
				"admin",
				"password");
		resourceList.addQueryParameter(
				"config_file",
				configFile.getAbsolutePath());
		String text = resourceList.get(
				MediaType.APPLICATION_JSON).getText();

		JSONParser parser = new JSONParser();
		JSONObject obj = (JSONObject) parser.parse(text);
		String name = (String) obj.get("store1");
		assertTrue(
				"store1 is removed",
				name == null);
	}

	// Tests geowave/config/addindex, cpindex, rmindex
	@Test
	public void geowave_config_index()
			throws ResourceException,
			IOException,
			ParseException {

		File configFile = tempFolder.newFile("test_config");

		// create a new index named "index1", with type "spatial"
		ClientResource resourceAdd = new ClientResource(
				"http://localhost:5152/geowave/config/addindex");
		resourceAdd.setChallengeResponse(
				ChallengeScheme.HTTP_BASIC,
				"admin",
				"password");
		Form formAdd = new Form();
		formAdd.add(
				"name",
				"index1");
		formAdd.add(
				"type",
				"spatial");
		formAdd.add(
				"default",
				"true");
		formAdd.add(
				"config_file",
				configFile.getAbsolutePath());
		resourceAdd.post(
				formAdd).write(
				System.out);

		// create a new index named "index2"
		ClientResource resourceCp = new ClientResource(
				"http://localhost:5152/geowave/config/cpindex");
		resourceCp.setChallengeResponse(
				ChallengeScheme.HTTP_BASIC,
				"admin",
				"password");
		Form formCp = new Form();
		formCp.add(
				"name",
				"index1");
		formCp.add(
				"newname",
				"index2");
		formCp.add(
				"default",
				"true");
		formCp.add(
				"config_file",
				configFile.getAbsolutePath());
		resourceCp.post(
				formCp).write(
				System.out);

		// remove the indexes named "index1" and "index2"
		ClientResource resourceRm = new ClientResource(
				"http://localhost:5152/geowave/config/rmindex");
		resourceRm.setChallengeResponse(
				ChallengeScheme.HTTP_BASIC,
				"admin",
				"password");
		Form formRm = new Form();
		formRm.add(
				"name",
				"index1");
		formRm.add(
				"config_file",
				configFile.getAbsolutePath());
		resourceRm.post(
				formRm).write(
				System.out);

		formRm.remove(0);
		formRm.add(
				"name",
				"index2");
		resourceRm.post(
				formRm).write(
				System.out);

		// use list to test if removed successfully
		ClientResource resourceList = new ClientResource(
				"http://localhost:5152/geowave/config/list");
		resourceList.setChallengeResponse(
				ChallengeScheme.HTTP_BASIC,
				"admin",
				"password");
		resourceList.addQueryParameter(
				"config_file",
				configFile.getAbsolutePath());

		String text = resourceList.get(
				MediaType.APPLICATION_JSON).getText();
		JSONParser parser = new JSONParser();
		JSONObject obj = (JSONObject) parser.parse(text);
		String name = (String) obj.get("index1");
		assertTrue(
				"index1 is removed",
				name == null);

	}

	// Tests geowave/config/addindexgrp, rmindexgrp
	@Test
	public void geowave_config_indexgrp()
			throws ResourceException,
			IOException,
			ParseException {

		File configFile = tempFolder.newFile("test_config");

		// create a new index named "index1", with type "spatial"
		ClientResource resourceAdd = new ClientResource(
				"http://localhost:5152/geowave/config/addindex");
		resourceAdd.setChallengeResponse(
				ChallengeScheme.HTTP_BASIC,
				"admin",
				"password");
		Form formAdd = new Form();
		formAdd.add(
				"name",
				"index1");
		formAdd.add(
				"type",
				"spatial");
		formAdd.add(
				"default",
				"true");
		formAdd.add(
				"config_file",
				configFile.getAbsolutePath());
		resourceAdd.post(
				formAdd).write(
				System.out);

		// create a new index named "index2"
		ClientResource resourceCp = new ClientResource(
				"http://localhost:5152/geowave/config/cpindex");
		resourceCp.setChallengeResponse(
				ChallengeScheme.HTTP_BASIC,
				"admin",
				"password");
		Form formCp = new Form();
		formCp.add(
				"name",
				"index1");
		formCp.add(
				"newname",
				"index2");
		formCp.add(
				"default",
				"true");
		formCp.add(
				"config_file",
				configFile.getAbsolutePath());
		resourceCp.post(
				formCp).write(
				System.out);

		// add the index group named "indexgrp1"
		ClientResource resourceAddGrp = new ClientResource(
				"http://localhost:5152/geowave/config/addindexgrp");
		resourceAddGrp.setChallengeResponse(
				ChallengeScheme.HTTP_BASIC,
				"admin",
				"password");
		Form formAddGrp = new Form();
		formAddGrp.add(
				"name",
				"indexgrp1");
		formAddGrp.add(
				"names",
				"index1,index2");
		formAddGrp.add(
				"config_file",
				configFile.getAbsolutePath());
		resourceAddGrp.post(
				formAddGrp).write(
				System.out);

		// remove the index group named "indexgrp"
		ClientResource resourceRm = new ClientResource(
				"http://localhost:5152/geowave/config/rmindexgrp");
		resourceRm.setChallengeResponse(
				ChallengeScheme.HTTP_BASIC,
				"admin",
				"password");
		Form formRm = new Form();
		formRm.add(
				"name",
				"indexgrp1");
		formRm.add(
				"config_file",
				configFile.getAbsolutePath());
		resourceRm.post(
				formRm).write(
				System.out);

		// use list to test if removed successfully
		ClientResource resourceList = new ClientResource(
				"http://localhost:5152/geowave/config/list");
		resourceList.setChallengeResponse(
				ChallengeScheme.HTTP_BASIC,
				"admin",
				"password");
		resourceList.addQueryParameter(
				"config_file",
				configFile.getAbsolutePath());
		String text = resourceList.get(
				MediaType.APPLICATION_JSON).getText();

		JSONParser parser = new JSONParser();
		JSONObject obj = (JSONObject) parser.parse(text);
		String name = (String) obj.get("indexgrp1");

		assertTrue(
				"indexgrp1 is removed",
				name == null);

	}

	// Ensures that calling an endpoint with a missing parameter is handled
	// correctly.
	@Test
	public void callWithMissingParameter()
			throws IOException,
			ParseException {
		File configFile = tempFolder.newFile("test_config");
		ClientResource resourceAdd = new ClientResource(
				"http://localhost:5152/geowave/config/addstore");
		resourceAdd.setChallengeResponse(
				ChallengeScheme.HTTP_BASIC,
				"admin",
				"password");
		Form formAdd = new Form();
		formAdd.add(
				"config_file",
				configFile.getAbsolutePath());

		try {
			resourceAdd.post(formAdd);
		}
		catch (ResourceException e) {
			assertEquals(
					e.getStatus(),
					Status.CLIENT_ERROR_BAD_REQUEST);
			JSONParser parser = new JSONParser();
			JSONObject obj = (JSONObject) parser.parse(resourceAdd.getResponseEntity().getText());
			assertEquals(
					obj.get("description"),
					"Missing argument: name");
			return;
		}

		fail("No exception was thrown");
	}

	// Test ingesting data into a newly created store with spatial index
	@Test
	public void geowave_ingest()
			throws IOException,
			ResourceException,
			ParseException {

		File configFile = tempFolder.newFile("test_config");

		// create a new store named "store1", with type "memory"
		ClientResource resourceAdd = new ClientResource(
				"http://localhost:5152/geowave/config/addstore");
		resourceAdd.setChallengeResponse(
				ChallengeScheme.HTTP_BASIC,
				"admin",
				"password");
		Form formAdd = new Form();
		formAdd.add(
				"name",
				"store1");
		formAdd.add(
				"storeType",
				"memory");
		formAdd.add(
				"default",
				"false");
		formAdd.add(
				"config_file",
				configFile.getAbsolutePath());
		resourceAdd.post(
				formAdd).write(
				System.out);

		// create index
		ClientResource resourceIndex = new ClientResource(
				"http://localhost:5152/geowave/config/addindex");
		resourceIndex.setChallengeResponse(
				ChallengeScheme.HTTP_BASIC,
				"admin",
				"password");
		Form formIndex = new Form();
		formIndex.add(
				"name",
				"index1");
		formIndex.add(
				"type",
				"spatial");
		formIndex.add(
				"default",
				"true");
		formIndex.add(
				"config_file",
				configFile.getAbsolutePath());
		resourceIndex.post(
				formIndex).write(
				System.out);

		// ingest data into store with index
		ClientResource resourceIngest = new ClientResource(
				"http://localhost:5152/geowave/ingest/localToGW");
		resourceIngest.setChallengeResponse(
				ChallengeScheme.HTTP_BASIC,
				"admin",
				"password");
		Form formIngest = new Form();
		formIngest.add(
				"path",
				"../../test/src/test/resources/mil/nga/giat/geowave/test/basic-testdata.zip");
		formIngest.add(
				"storename",
				"store1");
		formIngest.add(
				"indices",
				"index1");
		formIngest.add(
				"config_file",
				configFile.getAbsolutePath());
		resourceIngest.post(
				formIngest).write(
				System.out);

	}

	// Test uploading then ingesting data into a newly created store with
	// spatial index
	@Test
	public void geowave_upload_and_ingest()
			throws IOException,
			ResourceException,
			ParseException {

		File configFile = tempFolder.newFile("test_config");

		// create a new store named "store1", with type "memory"
		ClientResource resourceAdd = new ClientResource(
				"http://localhost:5152/geowave/config/addstore");
		resourceAdd.setChallengeResponse(
				ChallengeScheme.HTTP_BASIC,
				"admin",
				"password");
		Form formAdd = new Form();
		formAdd.add(
				"name",
				"store1");
		formAdd.add(
				"storeType",
				"memory");
		formAdd.add(
				"default",
				"false");
		formAdd.add(
				"config_file",
				configFile.getAbsolutePath());
		resourceAdd.post(
				formAdd).write(
				System.out);

		// create index
		ClientResource resourceIndex = new ClientResource(
				"http://localhost:5152/geowave/config/addindex");
		resourceIndex.setChallengeResponse(
				ChallengeScheme.HTTP_BASIC,
				"admin",
				"password");
		Form formIndex = new Form();
		formIndex.add(
				"name",
				"index1");
		formIndex.add(
				"type",
				"spatial");
		formIndex.add(
				"default",
				"true");
		formIndex.add(
				"config_file",
				configFile.getAbsolutePath());
		resourceIndex.post(
				formIndex).write(
				System.out);

		ClientResource resourceUpload = new ClientResource(
				"http://localhost:5152/fileupload");
		resourceUpload.setChallengeResponse(
				ChallengeScheme.HTTP_BASIC,
				"admin",
				"password");

		File file = new File(
				"../../test/src/test/resources/mil/nga/giat/geowave/test/basic-testdata.zip");
		Representation entity = new FileRepresentation(
				file,
				MediaType.MULTIPART_FORM_DATA);

		FormDataSet set = new FormDataSet();
		FormData data = new FormData(
				"file",
				entity);
		set.getEntries().add(
				data);
		set.setMultipart(true);

		String text = resourceUpload.post(
				set).getText();

		// Get name of uploaded file
		JSONParser parser = new JSONParser();
		JSONObject obj = (JSONObject) parser.parse(text);
		String name = (String) obj.get("name");
		System.out.println(">>>>>>>>>>>>>>>>>>>>> " + name);

		// ingest data into store with index
		ClientResource resourceIngest = new ClientResource(
				"http://localhost:5152/geowave/ingest/localToGW");
		resourceIngest.setChallengeResponse(
				ChallengeScheme.HTTP_BASIC,
				"admin",
				"password");
		Form formIngest = new Form();
		formIngest.add(
				"path",
				name); // specify uploaded file
		formIngest.add(
				"storename",
				"store1");
		formIngest.add(
				"indices",
				"index1");
		formIngest.add(
				"config_file",
				configFile.getAbsolutePath());
		resourceIngest.post(
				formIngest).write(
				System.out);

	}
}
