package mil.nga.giat.geowave.service.client;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.proxy.WebResourceFactory;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import mil.nga.giat.geowave.service.IngestService;

public class IngestServiceClient
{
	private final IngestService ingestService;

	public IngestServiceClient(
			final String baseUrl ) {
		this(
				baseUrl,
				null,
				null);
	}

	public IngestServiceClient(
			final String baseUrl,
			String user,
			String password ) {
		// ClientBuilder bldr = ClientBuilder.newBuilder();
		// if (user != null && password != null) {
		// HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic(
		// user,
		// password);
		// bldr.register(feature);
		// }
		ingestService = WebResourceFactory.newResource(
				IngestService.class,
				ClientBuilder.newClient().register(
						MultiPartFeature.class).target(
						baseUrl));
	}

	public Response listPlugins() {
		final Response resp = ingestService.listPlugins();
		resp.bufferEntity();
		return resp;
	}

	public Response kafkaToGW(
			final String store_name,
			final String index_group_list,// Array of Strings
			final String kafkaPropertyFile ) {

		return kafkaToGW(
				store_name,
				index_group_list,
				kafkaPropertyFile,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null);
	}

	public Response kafkaToGW(
			final String store_name,
			final String index_group_list,// Array of Strings
			final String kafkaPropertyFile,
			final String visibility,
			final String groupId,
			final String zookeeperConnect,
			final String autoOffsetReset,
			final String fetchMessageMaxBytes,
			final String consumerTimeoutMs,
			final Boolean reconnectOnTimeout,
			final Integer batchSize,
			final String extensions,// Array of Strings
			final String formats ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();

		multiPart.field(
				"store_name",
				store_name);
		multiPart.field(
				"index_group_list",
				index_group_list);
		multiPart.field(
				"kafkaPropertyFile",
				kafkaPropertyFile);
		if (visibility != null) {
			multiPart.field(
					"visibility",
					visibility);
		}
		if (groupId != null) {
			multiPart.field(
					"groupId",
					groupId);
		}
		if (zookeeperConnect != null) {
			multiPart.field(
					"zookeeperConnect",
					zookeeperConnect);
		}
		if (autoOffsetReset != null) {
			multiPart.field(
					"autoOffsetReset",
					autoOffsetReset);
		}
		if (fetchMessageMaxBytes != null) {
			multiPart.field(
					"fetchMessageMaxBytes",
					fetchMessageMaxBytes);
		}
		if (consumerTimeoutMs != null) {
			multiPart.field(
					"consumerTimeoutMs",
					consumerTimeoutMs);
		}
		if (reconnectOnTimeout != null) {
			multiPart.field(
					"reconnectOnTimeout",
					reconnectOnTimeout.toString());
		}
		if (batchSize != null) {
			multiPart.field(
					"batchSize",
					batchSize.toString());
		}
		if (extensions != null) {
			multiPart.field(
					"extensions",
					extensions);
		}
		if (formats != null) {
			multiPart.field(
					"formats",
					formats);
		}

		final Response resp = ingestService.kafkaToGW(multiPart);
		return resp;
	}

	public Response localToGW(
			final String file_or_directory,
			final String storename,
			final String index_group_list ) {

		return localToGW(
				file_or_directory,
				storename,
				index_group_list,
				null,
				null,
				null,
				null);
	}

	public Response localToGW(
			final String file_or_directory,
			final String storename,
			final String index_group_list,// Array of Strings
			final Integer threads,
			final String visibility,
			final String extensions, // Array of Strings
			final String formats ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();

		multiPart.field(
				"file_or_directory",
				file_or_directory);
		multiPart.field(
				"storename",
				storename);
		multiPart.field(
				"index_group_list",
				index_group_list);
		if (threads != null) {
			multiPart.field(
					"threads",
					threads.toString());
		}
		if (visibility != null) {
			multiPart.field(
					"visibility",
					visibility);
		}
		if (extensions != null) {
			multiPart.field(
					"extensions",
					extensions);
		}
		if (formats != null) {
			multiPart.field(
					"formats",
					formats);
		}

		final Response resp = ingestService.localToGW(multiPart);
		return resp;
	}

	public Response localToHdfs(
			final String file_or_directory,
			final String path_to_base_directory_to_write_to ) {

		return localToHdfs(
				file_or_directory,
				path_to_base_directory_to_write_to,
				null,
				null);
	}

	public Response localToHdfs(
			final String file_or_directory,
			final String path_to_base_directory_to_write_to,
			final String extensions, // Array of Strings
			final String formats ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();

		multiPart.field(
				"file_or_directory",
				file_or_directory);
		multiPart.field(
				"path_to_base_directory_to_write_to",
				path_to_base_directory_to_write_to);

		if (extensions != null) {
			multiPart.field(
					"extensions",
					extensions);
		}
		if (formats != null) {
			multiPart.field(
					"formats",
					formats);
		}

		final Response resp = ingestService.localToHdfs(multiPart);
		return resp;
	}

	public Response localToKafka(
			final String file_or_directory,
			final String kafkaPropertyFile,
			final String metadataBrokerList,
			final String requestRequiredAcks,
			final String producerType,
			final String serializerClass,
			final String retryBackoffMs,
			final String extensions, // Array of Strings
			final String formats ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();

		multiPart.field(
				"file_or_directory",
				file_or_directory);
		multiPart.field(
				"kafkaPropertyFile",
				kafkaPropertyFile);
		if (metadataBrokerList != null) {
			multiPart.field(
					"metadataBrokerList",
					metadataBrokerList);
		}
		if (requestRequiredAcks != null) {
			multiPart.field(
					"requestRequiredAcks",
					requestRequiredAcks);
		}
		if (producerType != null) {
			multiPart.field(
					"producerType",
					producerType);
		}
		if (serializerClass != null) {
			multiPart.field(
					"serializerClass",
					serializerClass);
		}
		if (retryBackoffMs != null) {
			multiPart.field(
					"retryBackoffMs",
					retryBackoffMs);
		}
		if (extensions != null) {
			multiPart.field(
					"extensions",
					extensions);
		}
		if (formats != null) {
			multiPart.field(
					"formats",
					formats);
		}

		final Response resp = ingestService.localToKafka(multiPart);
		return resp;
	}

	public Response localToKafka(
			final String file_or_directory,
			final String kafkaPropertyFile ) {

		return localToKafka(
				file_or_directory,
				kafkaPropertyFile,
				null,
				null,
				null,
				null,
				null,
				null,
				null);
	}

	public Response localToMrGW(
			final String file_or_directory,
			final String path_to_base_directory_to_write_to,
			final String store_name,
			final String index_group_list,// Array of Strings
			final String visibility,
			final String jobTrackerHostPort,
			final String resourceManger,
			final String extensions,// Array of Strings
			final String formats ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();

		multiPart.field(
				"file_or_directory",
				file_or_directory);
		multiPart.field(
				"path_to_base_directory_to_write_to",
				path_to_base_directory_to_write_to);
		multiPart.field(
				"store_name",
				store_name);
		multiPart.field(
				"index_group_list",
				index_group_list);
		if (visibility != null) {
			multiPart.field(
					"visibility",
					visibility);
		}
		if (jobTrackerHostPort != null) {
			multiPart.field(
					"jobTrackerHostPort",
					jobTrackerHostPort);
		}
		if (resourceManger != null) {
			multiPart.field(
					"resourceManger",
					resourceManger);
		}
		if (extensions != null) {
			multiPart.field(
					"extensions",
					extensions);
		}
		if (formats != null) {
			multiPart.field(
					"formats",
					formats);
		}
		final Response resp = ingestService.localToMrGW(multiPart);
		return resp;
	}

	public Response localToMrGW(
			final String file_or_directory,
			final String path_to_base_directory_to_write_to,
			final String store_name,
			final String index_group_list ) {// Array of Strings

		return localToMrGW(
				file_or_directory,
				path_to_base_directory_to_write_to,
				store_name,
				index_group_list,
				null,
				null,
				null,
				null,
				null);
	}

	public Response mrToGW(
			final String path_to_base_directory_to_write_to,
			final String store_name,
			final String index_group_list,// Array of Strings
			final String visibility,
			final String jobTrackerHostPort,
			final String resourceManger,
			final String extensions,// Array of Strings
			final String formats ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();

		multiPart.field(
				"path_to_base_directory_to_write_to",
				path_to_base_directory_to_write_to);
		multiPart.field(
				"store_name",
				store_name);
		multiPart.field(
				"index_group_list",
				index_group_list);
		if (visibility != null) {
			multiPart.field(
					"visibility",
					visibility);
		}
		if (jobTrackerHostPort != null) {
			multiPart.field(
					"jobTrackerHostPort",
					jobTrackerHostPort);
		}
		if (resourceManger != null) {
			multiPart.field(
					"resourceManger",
					resourceManger);
		}
		if (extensions != null) {
			multiPart.field(
					"extensions",
					extensions);
		}
		if (formats != null) {
			multiPart.field(
					"formats",
					formats);
		}
		final Response resp = ingestService.mrToGW(multiPart);
		return resp;
	}

	public Response mrToGW(
			final String path_to_base_directory_to_write_to,
			final String store_name,
			final String index_group_list ) {// Array of Strings

		return mrToGW(
				path_to_base_directory_to_write_to,
				store_name,
				index_group_list,
				null,
				null,
				null,
				null,
				null);
	}

	public Response sparkToGW(
			final String input_directory,
			final String store_name,
			final String index_group_list,// Array of Strings
			final String visibility,
			final String appName,
			final String host,
			final String master,
			final Integer numExecutors,
			final Integer numCores,
			final String extensions,// Array of Strings
			final String formats ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();

		multiPart.field(
				"input_directory",
				input_directory);
		multiPart.field(
				"store_name",
				store_name);
		multiPart.field(
				"index_group_list",
				index_group_list);
		if (visibility != null) {
			multiPart.field(
					"visibility",
					visibility);
		}
		if (appName != null) {
			multiPart.field(
					"appName",
					appName);
		}
		if (host != null) {
			multiPart.field(
					"host",
					host);
		}
		if (master != null) {
			multiPart.field(
					"master",
					master);
		}
		if (numExecutors != null) {
			multiPart.field(
					"numExecutors",
					numExecutors.toString());
		}
		if (numCores != null) {
			multiPart.field(
					"numCores",
					numCores.toString());
		}
		if (extensions != null) {
			multiPart.field(
					"extensions",
					extensions);
		}
		if (formats != null) {
			multiPart.field(
					"formats",
					formats);
		}
		final Response resp = ingestService.sparkToGW(multiPart);
		return resp;
	}

	public Response sparkToGW(
			final String input_directory,
			final String store_name,
			final String index_group_list ) {
		return sparkToGW(
				input_directory,
				store_name,
				index_group_list,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null);
	}
}
