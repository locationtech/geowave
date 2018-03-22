package mil.nga.giat.geowave.service;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Produces(MediaType.APPLICATION_JSON)
@Path("/v0/ingest")
public interface IngestService
{

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/listplugins")
	public Response listPlugins();

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/kafkaToGW")
	public Response kafkaToGW(
			@QueryParam("store_name") String store_name,
			@QueryParam("index_group_list") String index_group_list,// Array of
																	// Strings
			@QueryParam("kafkaPropertyFile") String kafkaPropertyFile,
			@QueryParam("visibility") String visibility,
			@QueryParam("groupId") String groupId,
			@QueryParam("zookeeperConnect") String zookeeperConnect,
			@QueryParam("autoOffsetReset") String autoOffsetReset,
			@QueryParam("fetchMessageMaxBytes") String fetchMessageMaxBytes,
			@QueryParam("consumerTimeoutMs") String consumerTimeoutMs,
			@QueryParam("reconnectOnTimeout") Boolean reconnectOnTimeout,
			@QueryParam("batchSize") Integer batchSize,
			@QueryParam("extensions") String extensions,// Array of Strings
			@QueryParam("formats") String formats );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/localToGW")
	public Response localToGW(
			@QueryParam("file_or_directory") String file_or_directory,
			@QueryParam("storename") String storename,
			@QueryParam("index_group_list") String index_group_list,// Array of
																	// Strings
			@QueryParam("threads") Integer threads,
			@QueryParam("visibility") String visibility,
			@QueryParam("extensions") String extensions, // Array of Strings
			@QueryParam("formats") String formats );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/localToHdfs")
	public Response localToHdfs(
			@QueryParam("file_or_directory") String file_or_directory,
			@QueryParam("path_to_base_directory_to_write_to") String path_to_base_directory_to_write_to,
			@QueryParam("extensions") String extensions, // Array of Strings
			@QueryParam("formats") String formats );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/localToKafka")
	public Response localToKafka(
			@QueryParam("file_or_directory") String file_or_directory,
			@QueryParam("kafkaPropertyFile") String kafkaPropertyFile,
			@QueryParam("metadataBrokerList") String metadataBrokerList,
			@QueryParam("requestRequiredAcks") String requestRequiredAcks,
			@QueryParam("producerType") String producerType,
			@QueryParam("serializerClass") String serializerClass,
			@QueryParam("retryBackoffMs") String retryBackoffMs,
			@QueryParam("extensions") String extensions, // Array of Strings
			@QueryParam("formats") String formats );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/localToMrGW")
	public Response localToMrGW(
			@QueryParam("file_or_directory") String file_or_directory,
			@QueryParam("path_to_base_directory_to_write_to") String path_to_base_directory_to_write_to,
			@QueryParam("store_name") String store_name,
			@QueryParam("index_group_list") String index_group_list,// Array of
																	// Strings
			@QueryParam("visibility") String visibility,
			@QueryParam("jobTrackerHostPort") String jobTrackerHostPort,
			@QueryParam("resourceManger") String resourceManger,
			@QueryParam("extensions") String extensions,// Array of Strings
			@QueryParam("formats") String formats );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/mrToGW")
	public Response mrToGW(
			@QueryParam("path_to_base_directory_to_write_to") String path_to_base_directory_to_write_to,
			@QueryParam("store_name") String store_name,
			@QueryParam("index_group_list") String index_group_list,// Array of
																	// Strings
			@QueryParam("visibility") String visibility,
			@QueryParam("jobTrackerHostPort") String jobTrackerHostPort,
			@QueryParam("resourceManger") String resourceManger,
			@QueryParam("extensions") String extensions,// Array of Strings
			@QueryParam("formats") String formats );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/sparkToGW")
	public Response sparkToGW(
			@QueryParam("input_directory") String input_directory,
			@QueryParam("store_name") String store_name,
			@QueryParam("index_group_list") String index_group_list,// Array of
																	// Strings
			@QueryParam("visibility") String visibility,
			@QueryParam("appName") String appName,
			@QueryParam("host") String host,
			@QueryParam("master") String master,
			@QueryParam("numExecutors") Integer numExecutors,
			@QueryParam("numCores") Integer numCores,
			@QueryParam("extensions") String extensions,// Array of Strings
			@QueryParam("formats") String formats );
}
