package mil.nga.giat.geowave.services.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

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

import mil.nga.giat.geowave.ingest.IngestMain;
import mil.nga.giat.geowave.services.IngestService;
import mil.nga.giat.geowave.services.utils.ServiceUtils;

import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;

import com.google.common.io.Files;

@Produces(MediaType.APPLICATION_JSON)
@Path("/ingest")
public class IngestServiceImpl implements
		IngestService
{
	private final String zookeeperUrl;
	private final String instance;
	private final String username;
	private final String password;
	private final String hdfs;
	private final String hdfsBase;
	private final String jobTracker;

	public IngestServiceImpl(
			@Context final ServletConfig servletConfig ) {
		final Properties props = ServiceUtils.loadProperties(servletConfig.getServletContext().getResourceAsStream(
				servletConfig.getInitParameter("config.properties")));

		zookeeperUrl = ServiceUtils.getProperty(
				props,
				"zookeeper.url");

		instance = ServiceUtils.getProperty(
				props,
				"zookeeper.instance");

		username = ServiceUtils.getProperty(
				props,
				"zookeeper.username");

		password = ServiceUtils.getProperty(
				props,
				"zookeeper.password");

		hdfs = ServiceUtils.getProperty(
				props,
				"hdfs");

		hdfsBase = ServiceUtils.getProperty(
				props,
				"hdfsBase");

		jobTracker = ServiceUtils.getProperty(
				props,
				"jobTracker");
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
		ingest(
				"hdfsingest",
				multiPart);
		return Response.ok().build();
	}

	private void ingest(
			final String ingestMethod,
			final FormDataMultiPart multiPart ) {

		// read the list of files
		final List<FormDataBodyPart> fields = multiPart.getFields("file");
		final Map<String, InputStream> fileMap = new HashMap<String, InputStream>();
		for (final FormDataBodyPart field : fields) {
			fileMap.put(
					field.getFormDataContentDisposition().getFileName(),
					field.getValueAs(InputStream.class));
		}

		final String namespace = multiPart.getField(
				"namespace").getValue();
		final String visibility = (multiPart.getField("visibility") != null) ? multiPart.getField(
				"visibility").getValue() : null;
		final String ingestType = (multiPart.getField("ingestType") != null) ? multiPart.getField(
				"ingestType").getValue() : "geotools-vector";
		final String dimType = (multiPart.getField("dimType") != null) ? multiPart.getField(
				"dimType").getValue() : "spatial";
		final boolean clear = (multiPart.getField("clear") != null) ? Boolean.parseBoolean(multiPart.getField(
				"clear").getValue()) : false;

		if ((namespace == null) || namespace.isEmpty()) {
			throw new WebApplicationException(
					Response.status(
							Status.BAD_REQUEST).entity(
							"Ingest Failed - Missing Namespace").build());
		}

		final File baseDir = Files.createTempDir();

		final Set<String> filenames = fileMap.keySet();
		for (final String filename : filenames) {
			final File tempFile = new File(
					baseDir,
					filename);

			// read the file
			try (OutputStream fileOutputStream = new FileOutputStream(
					tempFile)) {

				final InputStream inStream = fileMap.get(filename);

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
		runIngest(
				baseDir,
				ingestMethod,
				ingestType,
				dimType,
				namespace,
				visibility,
				clear);
	}

	private void runIngest(
			final File baseDir,
			final String ingestMethod,
			final String ingestType,
			final String dimType,
			final String namespace,
			final String visibility,
			final boolean clear ) {

		final ArrayList<String> args = new ArrayList<String>();
		args.add("-" + ingestMethod);
		args.add("-t");
		args.add(ingestType);
		args.add("-b");
		args.add(baseDir.getAbsolutePath());
		args.add("-z");
		args.add(zookeeperUrl);
		args.add("-n");
		args.add(namespace);
		args.add("-u");
		args.add(username);
		args.add("-p");
		args.add(password);
		args.add("-i");
		args.add(instance);
		args.add("-dim");
		args.add(dimType);

		if ((visibility != null) && !visibility.isEmpty()) {
			args.add("-v");
			args.add(visibility);
		}

		if (clear) {
			args.add("-c");
		}

		if (ingestMethod.equals("hdfsingest")) {
			args.add("-hdfs");
			args.add(hdfs);
			args.add("-hdfsbase");
			args.add(hdfsBase);
			args.add("-jobtracker");
			args.add(jobTracker);
		}

		IngestMain.main(args.toArray(new String[] {}));
	}
}