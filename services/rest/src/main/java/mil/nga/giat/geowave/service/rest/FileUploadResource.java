package mil.nga.giat.geowave.service.rest;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.ext.fileupload.RestletFileUpload;
import org.restlet.representation.Representation;
import org.restlet.resource.Post;
import org.restlet.resource.ResourceException;
import org.restlet.resource.ServerResource;

/**
 * ServerResource to handle uploading files. Uses restlet fileupload.
 */
public class FileUploadResource extends
		ServerResource
{
	private static final String KEY_BATCH_UUID = "batchUUID";

	private static class UploadedFile
	{
		private final String name;

		UploadedFile(
				final String name ) {
			this.name = name;
		}

		public String getName() {
			return name;
		}
	}

	/**
	 * processes uploaded file, storing in a temporary directory
	 *
	 * @param entity
	 * @return the directory storing the uploaded file
	 * @throws Exception
	 */
	@Post
	public UploadedFile accept(
			final Representation entity )
			throws Exception {
		UploadedFile result;
		if (isMediaType(
				entity,
				MediaType.MULTIPART_FORM_DATA)) {
			// 1/ Create a factory for disk-based file items
			final DiskFileItemFactory factory = new DiskFileItemFactory();
			factory.setSizeThreshold(1000240);

			// 2/ Create a new file upload handler based on the Restlet
			// FileUpload extension that will parse Restlet requests and
			// generates FileItems.
			final RestletFileUpload upload = new RestletFileUpload(
					factory);

			final List<FileItem> fileList = upload.parseRepresentation(entity);
			if (fileList.size() != 1) {
				throw new ResourceException(
						Status.CLIENT_ERROR_BAD_REQUEST);
			}
			FileItem item = fileList.get(0);
			// 3/ Request is parsed by the handler which generates a
			// list of FileItems
			final String tempDir = System.getProperty("java.io.tmpdir");
			final Path batchDir = Files.createDirectories(Paths.get(
					tempDir,
					createBatchDirname()));

			// HP Fortify "Path Traversal" false positive
			// A user would need to have OS-level access anyway
			// to change the system properties
			final File filename = Files.createTempFile(
					batchDir,
					"",
					"." + item.getName()).toFile();
			result = new UploadedFile(
					filename.getAbsolutePath());
			FileUtils.copyInputStreamToFile(
					item.getInputStream(),
					filename);
		}
		else {
			throw new ResourceException(
					Status.CLIENT_ERROR_UNSUPPORTED_MEDIA_TYPE);
		}

		return result;
	}

	private boolean isMediaType(
			Representation entity,
			MediaType desiredType ) {
		if (entity == null) {
			return false;
		}
		return desiredType.equals(
				entity.getMediaType(),
				true);
	}

	private String createBatchDirname() {
		final UUID uuid;
		final String provided = StringUtils.trimToEmpty(getQueryValue(KEY_BATCH_UUID));
		if (provided.isEmpty()) {
			uuid = UUID.randomUUID();
		}
		else {
			try {
				uuid = UUID.fromString(provided);
			}
			catch (IllegalArgumentException e) {
				throw new ResourceException(
						Status.CLIENT_ERROR_BAD_REQUEST,
						String.format(
								"'%s' must be a valid UUID",
								KEY_BATCH_UUID));
			}
		}

		return String.format(
				"upload-batch.%s",
				uuid);
	}
}
