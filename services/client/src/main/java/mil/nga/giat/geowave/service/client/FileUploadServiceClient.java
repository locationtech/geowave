package mil.nga.giat.geowave.service.client;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.proxy.WebResourceFactory;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;

import mil.nga.giat.geowave.service.FileUploadService;

import java.io.File;

public class FileUploadServiceClient
{
	private final FileUploadService fileUploadService;

	public FileUploadServiceClient(
			final String baseUrl ) {
		this(
				baseUrl,
				null,
				null);
	}

	public FileUploadServiceClient(
			final String baseUrl,
			String user,
			String password ) {

		fileUploadService = WebResourceFactory.newResource(
				FileUploadService.class,
				ClientBuilder.newClient().register(
						MultiPartFeature.class).target(
						baseUrl));
	}

	public Response uploadFile(
			final String file_path ) {

		final FileDataBodyPart filePart = new FileDataBodyPart(
				"file",
				new File(
						file_path));

		final FormDataMultiPart multiPart = new FormDataMultiPart();

		multiPart.bodyPart(filePart);

		final Response resp = fileUploadService.uploadFile(multiPart);

		return resp;
	}

}