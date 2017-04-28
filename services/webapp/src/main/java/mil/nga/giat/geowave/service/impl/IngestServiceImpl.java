package mil.nga.giat.geowave.service.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;

import com.google.common.io.Files;

import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.ingest.operations.LocalToGeowaveCommand;
import mil.nga.giat.geowave.core.ingest.operations.LocalToMapReduceToGeowaveCommand;
import mil.nga.giat.geowave.core.ingest.operations.options.IngestFormatPluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.service.IngestService;
import mil.nga.giat.geowave.service.ServiceUtils;

@Produces(MediaType.APPLICATION_JSON)
@Path("/ingest")
public class IngestServiceImpl implements
		IngestService
{
	private final static Logger LOGGER = LoggerFactory.getLogger(IngestServiceImpl.class);
	private final Properties serviceProperties;
	private final String hdfs;
	private final String hdfsBase;
	private final String jobTracker;

	public IngestServiceImpl(
			@Context
			final ServletConfig servletConfig ) {
		Properties props = null;
		try (InputStream is = servletConfig.getServletContext().getResourceAsStream(
				servletConfig.getInitParameter("config.properties"))) {
			props = ServiceUtils.loadProperties(is);
		}
		catch (IOException e) {
			LOGGER.error(
					e.getLocalizedMessage(),
					e);
		}

		hdfs = ServiceUtils.getProperty(
				props,
				"hdfs");

		hdfsBase = ServiceUtils.getProperty(
				props,
				"hdfsBase");

		jobTracker = ServiceUtils.getProperty(
				props,
				"jobTracker");
		serviceProperties = props;
	}

	@Override
	@POST
	@Path("/local")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Produces(MediaType.APPLICATION_JSON)
	public Response localIngest(
			final FormDataMultiPart multiPart ) {
		ingest(
				"localingest",
				multiPart);
		return Response.ok().build();
	}

	@Override
	@POST
	@Path("/hdfs")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Produces(MediaType.APPLICATION_JSON)
	public Response hdfsIngest(
			final FormDataMultiPart multiPart ) {
		return ingest(
				"hdfsingest",
				multiPart);

	}

	private Response ingest(
			final String ingestMethod,
			final FormDataMultiPart multiPart ) {

		final List<FormDataBodyPart> fileFields = multiPart.getFields("file");
		if (fileFields == null) {
			return Response.noContent().build();
		}

		// read the list of files
		final List<FormDataBodyPart> fields = fileFields;
		final Map<String, InputStream> fileMap = new HashMap<String, InputStream>();
		for (final FormDataBodyPart field : fields) {
			fileMap.put(
					field.getFormDataContentDisposition().getFileName(),
					field.getValueAs(InputStream.class));
		}

		final String storeName = multiPart.getField(
				"store").getValue();

		final String visibility = (multiPart.getField("visibility") != null) ? multiPart.getField(
				"visibility").getValue() : null;
		final String ingestType = (multiPart.getField("ingestFormat") != null) ? multiPart.getField(
				"ingestFormat").getValue() : "geotools-vector";
		final String dimType = (multiPart.getField("dimType") != null) ? multiPart.getField(
				"dimType").getValue() : "spatial";
		final boolean clear = (multiPart.getField("clear") != null) ? Boolean.parseBoolean(multiPart.getField(
				"clear").getValue()) : false;

		if ((storeName == null) || storeName.isEmpty()) {
			throw new WebApplicationException(
					Response.status(
							Status.BAD_REQUEST).entity(
							"Ingest Failed - Missing Store Name").build());
		}

		final File baseDir = Files.createTempDir();

		for (final Map.Entry<String, InputStream> kvp : fileMap.entrySet()) {
			final File tempFile = new File(
					baseDir,
					kvp.getKey());

			// read the file
			try (OutputStream fileOutputStream = new FileOutputStream(
					tempFile)) {

				final InputStream inStream = kvp.getValue();

				int read = 0;
				final byte[] bytes = new byte[1024];
				while ((read = inStream.read(bytes)) != -1) {
					fileOutputStream.write(
							bytes,
							0,
							read);
				}
			}
			catch (final IOException e) {
				throw new WebApplicationException(
						Response.status(
								Status.INTERNAL_SERVER_ERROR).entity(
								"Ingest Failed" + e.getMessage()).build());
			}
		}

		// ingest the files
		return runIngest(
				baseDir,
				ingestMethod,
				ingestType,
				dimType,
				storeName,
				visibility,
				clear);
	}

	private Response runIngest(
			final File baseDir,
			final String ingestMethod,
			final String ingestType,
			final String dimType,
			final String storeName,
			final String visibility,
			final boolean clear ) {

		// Ingest Formats
		final IngestFormatPluginOptions ingestFormatOptions = new IngestFormatPluginOptions();
		ingestFormatOptions.selectPlugin(ingestType);

		// Indexes
		final IndexPluginOptions indexOption = new IndexPluginOptions();
		indexOption.selectPlugin(dimType);

		// Store
		final String namespace = DataStorePluginOptions.getStoreNamespace(storeName);
		final DataStorePluginOptions dataStorePlugin = new DataStorePluginOptions();
		if (!dataStorePlugin.load(
				serviceProperties,
				namespace)) {
			throw new WebApplicationException(
					Response.status(
							Status.BAD_REQUEST).entity(
							"Ingest Failed - Invalid Store").build());
		}

		switch (ingestMethod) {
			default:
			case "localingest":
				final LocalToGeowaveCommand localIngester = new LocalToGeowaveCommand();
				localIngester.setPluginFormats(ingestFormatOptions);
				localIngester.setInputIndexOptions(Arrays.asList(indexOption));
				localIngester.setInputStoreOptions(dataStorePlugin);
				localIngester.getIngestOptions().setVisibility(
						visibility);
				localIngester.setParameters(
						baseDir.getAbsolutePath(),
						null,
						null);
				localIngester.prepare(new ManualOperationParams());
				localIngester.execute(new ManualOperationParams());
				return Response.ok().build();

			case "hdfsingest":
				final LocalToMapReduceToGeowaveCommand hdfsIngester = new LocalToMapReduceToGeowaveCommand();
				hdfsIngester.setPluginFormats(ingestFormatOptions);
				hdfsIngester.setInputIndexOptions(Arrays.asList(indexOption));
				hdfsIngester.setInputStoreOptions(dataStorePlugin);
				hdfsIngester.getIngestOptions().setVisibility(
						visibility);
				hdfsIngester.setParameters(
						baseDir.getAbsolutePath(),
						hdfs,
						hdfsBase,
						null,
						null);
				hdfsIngester.getMapReduceOptions().setJobTrackerHostPort(
						jobTracker);
				hdfsIngester.prepare(new ManualOperationParams());
				hdfsIngester.execute(new ManualOperationParams());
				return Response.ok().build();
		}
	}
}