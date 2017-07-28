package mil.nga.giat.geowave.service.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.restlet.data.ChallengeScheme;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.ext.html.FormData;
import org.restlet.ext.html.FormDataSet;
import org.restlet.representation.FileRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.ClientResource;
import org.restlet.resource.ResourceException;

import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;

public class RestServerTest
{
	// Do to the ongoing changes in dependency injection the Rest Server unit
	// tests will not currently pass
	// TODO Uncomment tests once dependency injection changes are completed
	/*
	 * @Rule public TemporaryFolder tempFolder = new TemporaryFolder();
	 * 
	 * @BeforeClass public static void runServer() { // Ensure the config
	 * directory exists. final File configDirectory =
	 * ConfigOptions.getDefaultPropertyPath(); if (!configDirectory.exists() &&
	 * !configDirectory.mkdir()) { throw new RuntimeException(
	 * "Unable to create directory " + configDirectory); } RestServer.main(new
	 * String[] {});
	 * 
	 * }
	 * 
	 * // Tests geowave/config/set and list
	 * 
	 * @Test public void geowave_config_set_list() throws ResourceException,
	 * IOException, ParseException {
	 * 
	 * final File configFile = tempFolder.newFile("test_config");
	 * 
	 * // create a new store named "store1", with type "memory" final
	 * ClientResource resourceAdd = new ClientResource(
	 * "http://localhost:5152/geowave/config/addstore");
	 * resourceAdd.setChallengeResponse( ChallengeScheme.HTTP_BASIC, "admin",
	 * "password"); final Form formAdd = new Form(); formAdd.add( "name",
	 * "store1"); formAdd.add( "storeType", "memory"); formAdd.add( "default",
	 * "false"); formAdd.add( "config_file", configFile.getAbsolutePath());
	 * resourceAdd.post( formAdd).write( System.out);
	 * 
	 * // set key=store1, value=store2 final ClientResource resourceSet = new
	 * ClientResource( "http://localhost:5152/geowave/config/set");
	 * resourceSet.setChallengeResponse( ChallengeScheme.HTTP_BASIC, "admin",
	 * "password"); final Form formSet = new Form(); formSet.add( "key",
	 * "store1"); formSet.add( "value", "store2"); formSet.add( "config_file",
	 * configFile.getAbsolutePath()); resourceSet.post( formSet).write(
	 * System.out);
	 * 
	 * // testing list final ClientResource resourceList = new ClientResource(
	 * "http://localhost:5152/geowave/config/list");
	 * resourceList.setChallengeResponse( ChallengeScheme.HTTP_BASIC, "admin",
	 * "password"); resourceList.addQueryParameter( "config_file",
	 * configFile.getAbsolutePath()); final String text = resourceList.get(
	 * MediaType.APPLICATION_JSON).getText();
	 * 
	 * final JSONParser parser = new JSONParser(); final JSONObject obj =
	 * (JSONObject) parser.parse(text); final String name = (String)
	 * obj.get("store1"); assertTrue( "'name' is 'value'",
	 * name.equals("store2"));
	 * 
	 * // remove the store named "store1" final ClientResource resourceRm = new
	 * ClientResource( "http://localhost:5152/geowave/config/rmstore");
	 * resourceRm.setChallengeResponse( ChallengeScheme.HTTP_BASIC, "admin",
	 * "password"); final Form formRm = new Form(); formRm.add( "name",
	 * "store1"); formRm.add( "config_file", configFile.getAbsolutePath());
	 * resourceRm.post( formRm).write( System.out); }
	 * 
	 * // Tests geowave/config/addstore, cpstore, rmstore
	 * 
	 * @Test public void geowave_config_store() throws ResourceException,
	 * IOException, ParseException {
	 * 
	 * final File configFile = tempFolder.newFile("test_config");
	 * 
	 * // create a new store named "store1", with type "memory" final
	 * ClientResource resourceAdd = new ClientResource(
	 * "http://localhost:5152/geowave/config/addstore");
	 * resourceAdd.setChallengeResponse( ChallengeScheme.HTTP_BASIC, "admin",
	 * "password"); final Form formAdd = new Form(); formAdd.add( "name",
	 * "store1"); formAdd.add( "storeType", "memory"); formAdd.add( "default",
	 * "true"); formAdd.add( "config_file", configFile.getAbsolutePath());
	 * resourceAdd.post( formAdd).write( System.out);
	 * 
	 * // create a new store named "store2" final ClientResource resourceCp =
	 * new ClientResource( "http://localhost:5152/geowave/config/cpstore");
	 * resourceCp.setChallengeResponse( ChallengeScheme.HTTP_BASIC, "admin",
	 * "password"); final Form formCp = new Form(); formCp.add( "name",
	 * "store1"); formCp.add( "newname", "store2"); formCp.add( "default",
	 * "true"); formCp.add( "config_file", configFile.getAbsolutePath());
	 * resourceCp.post( formCp).write( System.out);
	 * 
	 * // remove the store named "store1" and "store2" final ClientResource
	 * resourceRm = new ClientResource(
	 * "http://localhost:5152/geowave/config/rmstore");
	 * resourceRm.setChallengeResponse( ChallengeScheme.HTTP_BASIC, "admin",
	 * "password"); final Form formRm = new Form(); formRm.add( "name",
	 * "store1"); formRm.add( "config_file", configFile.getAbsolutePath());
	 * resourceRm.post( formRm).write( System.out);
	 * 
	 * formRm.remove(0); formRm.add( "name", "store2"); resourceRm.post(
	 * formRm).write( System.out);
	 * 
	 * // use list to test if removed successfully final ClientResource
	 * resourceList = new ClientResource(
	 * "http://localhost:5152/geowave/config/list");
	 * resourceList.setChallengeResponse( ChallengeScheme.HTTP_BASIC, "admin",
	 * "password"); resourceList.addQueryParameter( "config_file",
	 * configFile.getAbsolutePath()); final String text = resourceList.get(
	 * MediaType.APPLICATION_JSON).getText();
	 * 
	 * final JSONParser parser = new JSONParser(); final JSONObject obj =
	 * (JSONObject) parser.parse(text); final String name = (String)
	 * obj.get("store1"); assertTrue( "store1 is removed", name == null); }
	 * 
	 * // Tests geowave/config/addindex, cpindex, rmindex
	 * 
	 * @Test public void geowave_config_index() throws ResourceException,
	 * IOException, ParseException {
	 * 
	 * final File configFile = tempFolder.newFile("test_config");
	 * 
	 * // create a new index named "index1", with type "spatial" final
	 * ClientResource resourceAdd = new ClientResource(
	 * "http://localhost:5152/geowave/config/addindex");
	 * resourceAdd.setChallengeResponse( ChallengeScheme.HTTP_BASIC, "admin",
	 * "password"); final Form formAdd = new Form(); formAdd.add( "name",
	 * "index1"); formAdd.add( "type", "spatial"); formAdd.add( "default",
	 * "true"); formAdd.add( "config_file", configFile.getAbsolutePath());
	 * resourceAdd.post( formAdd).write( System.out);
	 * 
	 * // create a new index named "index2" final ClientResource resourceCp =
	 * new ClientResource( "http://localhost:5152/geowave/config/cpindex");
	 * resourceCp.setChallengeResponse( ChallengeScheme.HTTP_BASIC, "admin",
	 * "password"); final Form formCp = new Form(); formCp.add( "name",
	 * "index1"); formCp.add( "newname", "index2"); formCp.add( "default",
	 * "true"); formCp.add( "config_file", configFile.getAbsolutePath());
	 * resourceCp.post( formCp).write( System.out);
	 * 
	 * // remove the indexes named "index1" and "index2" final ClientResource
	 * resourceRm = new ClientResource(
	 * "http://localhost:5152/geowave/config/rmindex");
	 * resourceRm.setChallengeResponse( ChallengeScheme.HTTP_BASIC, "admin",
	 * "password"); final Form formRm = new Form(); formRm.add( "name",
	 * "index1"); formRm.add( "config_file", configFile.getAbsolutePath());
	 * resourceRm.post( formRm).write( System.out);
	 * 
	 * formRm.remove(0); formRm.add( "name", "index2"); resourceRm.post(
	 * formRm).write( System.out);
	 * 
	 * // use list to test if removed successfully final ClientResource
	 * resourceList = new ClientResource(
	 * "http://localhost:5152/geowave/config/list");
	 * resourceList.setChallengeResponse( ChallengeScheme.HTTP_BASIC, "admin",
	 * "password"); resourceList.addQueryParameter( "config_file",
	 * configFile.getAbsolutePath());
	 * 
	 * final String text = resourceList.get(
	 * MediaType.APPLICATION_JSON).getText(); final JSONParser parser = new
	 * JSONParser(); final JSONObject obj = (JSONObject) parser.parse(text);
	 * final String name = (String) obj.get("index1"); assertTrue(
	 * "index1 is removed", name == null);
	 * 
	 * }
	 * 
	 * // Tests geowave/config/addindexgrp, rmindexgrp
	 * 
	 * @Test public void geowave_config_indexgrp() throws ResourceException,
	 * IOException, ParseException {
	 * 
	 * final File configFile = tempFolder.newFile("test_config");
	 * 
	 * // create a new index named "index1", with type "spatial" final
	 * ClientResource resourceAdd = new ClientResource(
	 * "http://localhost:5152/geowave/config/addindex");
	 * resourceAdd.setChallengeResponse( ChallengeScheme.HTTP_BASIC, "admin",
	 * "password"); final Form formAdd = new Form(); formAdd.add( "name",
	 * "index1"); formAdd.add( "type", "spatial"); formAdd.add( "default",
	 * "true"); formAdd.add( "config_file", configFile.getAbsolutePath());
	 * resourceAdd.post( formAdd).write( System.out);
	 * 
	 * // create a new index named "index2" final ClientResource resourceCp =
	 * new ClientResource( "http://localhost:5152/geowave/config/cpindex");
	 * resourceCp.setChallengeResponse( ChallengeScheme.HTTP_BASIC, "admin",
	 * "password"); final Form formCp = new Form(); formCp.add( "name",
	 * "index1"); formCp.add( "newname", "index2"); formCp.add( "default",
	 * "true"); formCp.add( "config_file", configFile.getAbsolutePath());
	 * resourceCp.post( formCp).write( System.out);
	 * 
	 * // add the index group named "indexgrp1" final ClientResource
	 * resourceAddGrp = new ClientResource(
	 * "http://localhost:5152/geowave/config/addindexgrp");
	 * resourceAddGrp.setChallengeResponse( ChallengeScheme.HTTP_BASIC, "admin",
	 * "password"); final Form formAddGrp = new Form(); formAddGrp.add( "name",
	 * "indexgrp1"); formAddGrp.add( "names", "index1,index2"); formAddGrp.add(
	 * "config_file", configFile.getAbsolutePath()); resourceAddGrp.post(
	 * formAddGrp).write( System.out);
	 * 
	 * // remove the index group named "indexgrp" final ClientResource
	 * resourceRm = new ClientResource(
	 * "http://localhost:5152/geowave/config/rmindexgrp");
	 * resourceRm.setChallengeResponse( ChallengeScheme.HTTP_BASIC, "admin",
	 * "password"); final Form formRm = new Form(); formRm.add( "name",
	 * "indexgrp1"); formRm.add( "config_file", configFile.getAbsolutePath());
	 * resourceRm.post( formRm).write( System.out);
	 * 
	 * // use list to test if removed successfully final ClientResource
	 * resourceList = new ClientResource(
	 * "http://localhost:5152/geowave/config/list");
	 * resourceList.setChallengeResponse( ChallengeScheme.HTTP_BASIC, "admin",
	 * "password"); resourceList.addQueryParameter( "config_file",
	 * configFile.getAbsolutePath()); final String text = resourceList.get(
	 * MediaType.APPLICATION_JSON).getText();
	 * 
	 * final JSONParser parser = new JSONParser(); final JSONObject obj =
	 * (JSONObject) parser.parse(text); final String name = (String)
	 * obj.get("indexgrp1");
	 * 
	 * assertTrue( "indexgrp1 is removed", name == null);
	 * 
	 * }
	 * 
	 * // Ensures that calling an endpoint with a missing parameter is handled
	 * // correctly.
	 * 
	 * @Test public void callWithMissingParameter() throws IOException,
	 * ParseException { final File configFile =
	 * tempFolder.newFile("test_config"); final ClientResource resourceAdd = new
	 * ClientResource( "http://localhost:5152/geowave/config/addstore");
	 * resourceAdd.setChallengeResponse( ChallengeScheme.HTTP_BASIC, "admin",
	 * "password"); final Form formAdd = new Form(); formAdd.add( "config_file",
	 * configFile.getAbsolutePath());
	 * 
	 * try { resourceAdd.post(formAdd); } catch (final ResourceException e) {
	 * assertEquals( e.getStatus(), Status.CLIENT_ERROR_BAD_REQUEST); final
	 * JSONParser parser = new JSONParser(); final JSONObject obj = (JSONObject)
	 * parser.parse(resourceAdd.getResponseEntity().getText()); assertEquals(
	 * obj.get("description"), "Missing argument: name"); return; }
	 * 
	 * fail("No exception was thrown"); }
	 * 
	 * // Test ingesting data into a newly created store with spatial index
	 * 
	 * @Test public void geowave_ingest() throws IOException, ResourceException,
	 * ParseException {
	 * 
	 * final File configFile = tempFolder.newFile("test_config");
	 * 
	 * // create a new store named "store1", with type "memory" final
	 * ClientResource resourceAdd = new ClientResource(
	 * "http://localhost:5152/geowave/config/addstore");
	 * resourceAdd.setChallengeResponse( ChallengeScheme.HTTP_BASIC, "admin",
	 * "password"); final Form formAdd = new Form(); formAdd.add( "name",
	 * "store1"); formAdd.add( "storeType", "memory"); formAdd.add( "default",
	 * "false"); formAdd.add( "config_file", configFile.getAbsolutePath());
	 * resourceAdd.post( formAdd).write( System.out);
	 * 
	 * // create index final ClientResource resourceIndex = new ClientResource(
	 * "http://localhost:5152/geowave/config/addindex");
	 * resourceIndex.setChallengeResponse( ChallengeScheme.HTTP_BASIC, "admin",
	 * "password"); final Form formIndex = new Form(); formIndex.add( "name",
	 * "index1"); formIndex.add( "type", "spatial"); formIndex.add( "default",
	 * "true"); formIndex.add( "config_file", configFile.getAbsolutePath());
	 * resourceIndex.post( formIndex).write( System.out);
	 * 
	 * // ingest data into store with index final ClientResource resourceIngest
	 * = new ClientResource( "http://localhost:5152/geowave/ingest/localToGW");
	 * resourceIngest.setChallengeResponse( ChallengeScheme.HTTP_BASIC, "admin",
	 * "password"); final Form formIngest = new Form(); formIngest.add( "path",
	 * "../../test/src/test/resources/mil/nga/giat/geowave/test/basic-testdata.zip"
	 * ); formIngest.add( "storename", "store1"); formIngest.add( "indices",
	 * "index1"); formIngest.add( "config_file", configFile.getAbsolutePath());
	 * resourceIngest.post( formIngest).write( System.out);
	 * 
	 * }
	 * 
	 * // Test uploading then ingesting data into a newly created store with //
	 * spatial index
	 * 
	 * @Test public void geowave_upload_and_ingest() throws IOException,
	 * ResourceException, ParseException {
	 * 
	 * final File configFile = tempFolder.newFile("test_config");
	 * 
	 * // create a new store named "store1", with type "memory" final
	 * ClientResource resourceAdd = new ClientResource(
	 * "http://localhost:5152/geowave/config/addstore");
	 * resourceAdd.setChallengeResponse( ChallengeScheme.HTTP_BASIC, "admin",
	 * "password"); final Form formAdd = new Form(); formAdd.add( "name",
	 * "store1"); formAdd.add( "storeType", "memory"); formAdd.add( "default",
	 * "false"); formAdd.add( "config_file", configFile.getAbsolutePath());
	 * resourceAdd.post( formAdd).write( System.out);
	 * 
	 * // create index final ClientResource resourceIndex = new ClientResource(
	 * "http://localhost:5152/geowave/config/addindex");
	 * resourceIndex.setChallengeResponse( ChallengeScheme.HTTP_BASIC, "admin",
	 * "password"); final Form formIndex = new Form(); formIndex.add( "name",
	 * "index1"); formIndex.add( "type", "spatial"); formIndex.add( "default",
	 * "true"); formIndex.add( "config_file", configFile.getAbsolutePath());
	 * resourceIndex.post( formIndex).write( System.out);
	 * 
	 * final ClientResource resourceUpload = new ClientResource(
	 * "http://localhost:5152/fileupload"); resourceUpload.setChallengeResponse(
	 * ChallengeScheme.HTTP_BASIC, "admin", "password");
	 * 
	 * final File file = new File(
	 * "../../test/src/test/resources/mil/nga/giat/geowave/test/basic-testdata.zip"
	 * ); final Representation entity = new FileRepresentation( file,
	 * MediaType.MULTIPART_FORM_DATA);
	 * 
	 * final FormDataSet set = new FormDataSet(); final FormData data = new
	 * FormData( "file", entity); set.getEntries().add( data);
	 * set.setMultipart(true);
	 * 
	 * final String text = resourceUpload.post( set).getText();
	 * 
	 * // Get name of uploaded file final JSONParser parser = new JSONParser();
	 * final JSONObject obj = (JSONObject) parser.parse(text); final String name
	 * = (String) obj.get("name"); System.out.println(">>>>>>>>>>>>>>>>>>>>> " +
	 * name);
	 * 
	 * // ingest data into store with index final ClientResource resourceIngest
	 * = new ClientResource( "http://localhost:5152/geowave/ingest/localToGW");
	 * resourceIngest.setChallengeResponse( ChallengeScheme.HTTP_BASIC, "admin",
	 * "password"); final Form formIngest = new Form(); formIngest.add( "path",
	 * name); // specify uploaded file formIngest.add( "storename", "store1");
	 * formIngest.add( "indices", "index1"); formIngest.add( "config_file",
	 * configFile.getAbsolutePath()); resourceIngest.post( formIngest).write(
	 * System.out);
	 * 
	 * }
	 */
}
