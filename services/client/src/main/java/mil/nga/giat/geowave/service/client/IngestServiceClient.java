package mil.nga.giat.geowave.service.client;

import java.io.File;
import java.io.FileNotFoundException;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import mil.nga.giat.geowave.service.IngestService;

import org.glassfish.jersey.client.proxy.WebResourceFactory;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;

public class IngestServiceClient
{
	private final IngestService ingestService;

	public IngestServiceClient(
			final String baseUrl ) {
		ingestService = WebResourceFactory.newResource(
				IngestService.class,
				ClientBuilder.newBuilder().register(
						MultiPartFeature.class).build().target(
						baseUrl));
	}

	public boolean localIngest(
			final File[] inputFiles,
			final String namespace )
			throws FileNotFoundException {
		return localIngest(
				inputFiles,
				namespace,
				null,
				null,
				null,
				false);
	};

	public boolean localIngest(
			final File[] inputFiles,
			final String namespace,
			final String visibility )
			throws FileNotFoundException {
		return localIngest(
				inputFiles,
				namespace,
				visibility,
				null,
				null,
				false);
	};

	public boolean localIngest(
			final File[] inputFiles,
			final String namespace,
			final String visibility,
			final String ingestFormat,
			final String dimType,
			final boolean clear )
			throws FileNotFoundException {
		final FormDataMultiPart multiPart = new FormDataMultiPart();

		for (final File file : inputFiles) {
			multiPart.bodyPart(new FileDataBodyPart(
					"file",
					file));
		}

		multiPart.field(
				"namespace",
				namespace);

		if (visibility != null) {
			multiPart.field(
					"visibility",
					visibility);
		}

		if (ingestFormat != null) {
			multiPart.field(
					"ingestFormat",
					ingestFormat);
		}

		if (dimType != null) {
			multiPart.field(
					"dimType",
					dimType);
		}

		if (clear) {
			multiPart.field(
					"clear",
					Boolean.toString(clear));
		}

		final Response resp = ingestService.localIngest(multiPart);
		return resp.getStatus() == Status.OK.getStatusCode();
	}

	public boolean hdfsIngest(
			final File[] inputFiles,
			final String namespace )
			throws FileNotFoundException {
		return hdfsIngest(
				inputFiles,
				namespace,
				null,
				null,
				null,
				false);
	};

	public boolean hdfsIngest(
			final File[] inputFiles,
			final String namespace,
			final String visibility )
			throws FileNotFoundException {
		return hdfsIngest(
				inputFiles,
				namespace,
				visibility,
				null,
				null,
				false);
	};

	public boolean hdfsIngest(
			final File[] inputFiles,
			final String namespace,
			final String visibility,
			final String ingestFormat,
			final String dimType,
			final boolean clear )
			throws FileNotFoundException {
		final FormDataMultiPart multiPart = new FormDataMultiPart();

		for (final File file : inputFiles) {
			multiPart.bodyPart(new FileDataBodyPart(
					"file",
					file));
		}

		multiPart.field(
				"namespace",
				namespace);

		if (visibility != null) {
			multiPart.field(
					"visibility",
					visibility);
		}

		if (ingestFormat != null) {
			multiPart.field(
					"ingestFormat",
					ingestFormat);
		}

		if (dimType != null) {
			multiPart.field(
					"dimType",
					dimType);
		}

		if (clear) {
			multiPart.field(
					"clear",
					Boolean.toString(clear));
		}

		final Response resp = ingestService.hdfsIngest(multiPart);
		return resp.getStatus() == Status.OK.getStatusCode();
	}
}
