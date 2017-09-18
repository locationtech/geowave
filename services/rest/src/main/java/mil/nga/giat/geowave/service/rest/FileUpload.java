package mil.nga.giat.geowave.service.rest;

import java.io.File;
import java.util.List;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.io.FileUtils;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.ext.fileupload.RestletFileUpload;
import org.restlet.representation.Representation;
import org.restlet.resource.Post;
import org.restlet.resource.ResourceException;
import org.restlet.resource.ServerResource;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;

/**
 * ServerResource to handle uploading files. Uses restlet fileupload.
 */
public class FileUpload extends
		ServerResource
{
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
		if ((entity != null) && MediaType.MULTIPART_FORM_DATA.equals(
				entity.getMediaType(),
				true)) {
			// 1/ Create a factory for disk-based file items
			final DiskFileItemFactory factory = new DiskFileItemFactory();
			factory.setSizeThreshold(1000240);

			// 2/ Create a new file upload handler based on the Restlet
			// FileUpload extension that will parse Restlet requests and
			// generates FileItems.
			final RestletFileUpload upload = new RestletFileUpload(
					factory);

			// 3/ Request is parsed by the handler which generates a
			// list of FileItems
			final String tempDir = System.getProperty("java.io.tmpdir");
			final File dir = new File(
					tempDir);
			final File filename = File.createTempFile(
					"uploadedfile",
					".tmp",
					dir);
			result = new UploadedFile(
					filename.getAbsolutePath());
			final List<FileItem> fileList = upload.parseRepresentation(entity);
			if (fileList.size() != 1) {
				throw new ResourceException(
						Status.CLIENT_ERROR_BAD_REQUEST);
			}
			FileUtils.copyInputStreamToFile(
					fileList.get(
							0).getInputStream(),
					filename);
		}
		else {
			throw new ResourceException(
					Status.CLIENT_ERROR_UNSUPPORTED_MEDIA_TYPE);
		}

		return result;
	}

}
