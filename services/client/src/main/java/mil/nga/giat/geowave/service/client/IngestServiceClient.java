package mil.nga.giat.geowave.service.client;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.proxy.WebResourceFactory;
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
			final String index_group_list,
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
			final String index_group_list,
			final String kafkaPropertyFile,
			final String visibility,
			final String groupId,
			final String zookeeperConnect,
			final String autoOffsetReset,
			final String fetchMessageMaxBytes,
			final String consumerTimeoutMs,
			final Boolean reconnectOnTimeout,
			final Integer batchSize,
			final String extensions,
			final String formats ) {

		final Response resp = ingestService.kafkaToGW(
				store_name,
				index_group_list,
				kafkaPropertyFile,
				visibility,
				groupId,
				zookeeperConnect,
				autoOffsetReset,
				fetchMessageMaxBytes,
				consumerTimeoutMs,
				reconnectOnTimeout,
				batchSize,
				extensions,
				formats);
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
			final String index_group_list,
			final Integer threads,
			final String visibility,
			final String extensions,
			final String formats ) {

		final Response resp = ingestService.localToGW(
				file_or_directory,
				storename,
				index_group_list,
				threads,
				visibility,
				extensions,
				formats);
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
			final String extensions,
			final String formats ) {

		final Response resp = ingestService.localToHdfs(
				file_or_directory,
				path_to_base_directory_to_write_to,
				extensions,
				formats);
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
			final String extensions,
			final String formats ) {

		final Response resp = ingestService.localToKafka(
				file_or_directory,
				kafkaPropertyFile,
				metadataBrokerList,
				requestRequiredAcks,
				producerType,
				serializerClass,
				retryBackoffMs,
				extensions,
				formats);
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
			final String index_group_list,
			final String visibility,
			final String jobTrackerHostPort,
			final String resourceManger,
			final String extensions,
			final String formats ) {

		final Response resp = ingestService.localToMrGW(
				file_or_directory,
				path_to_base_directory_to_write_to,
				store_name,
				index_group_list,
				visibility,
				jobTrackerHostPort,
				resourceManger,
				extensions,
				formats);
		return resp;
	}

	public Response localToMrGW(
			final String file_or_directory,
			final String path_to_base_directory_to_write_to,
			final String store_name,
			final String index_group_list ) {

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
			final String index_group_list,
			final String visibility,
			final String jobTrackerHostPort,
			final String resourceManger,
			final String extensions,
			final String formats ) {

		final Response resp = ingestService.mrToGW(
				path_to_base_directory_to_write_to,
				store_name,
				index_group_list,
				visibility,
				jobTrackerHostPort,
				resourceManger,
				extensions,
				formats);
		return resp;
	}

	public Response mrToGW(
			final String path_to_base_directory_to_write_to,
			final String store_name,
			final String index_group_list ) {

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
			final String index_group_list,
			final String visibility,
			final String appName,
			final String host,
			final String master,
			final Integer numExecutors,
			final Integer numCores,
			final String extensions,
			final String formats ) {

		final Response resp = ingestService.sparkToGW(
				input_directory,
				store_name,
				index_group_list,
				visibility,
				appName,
				host,
				master,
				numExecutors,
				numCores,
				extensions,
				formats);
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
