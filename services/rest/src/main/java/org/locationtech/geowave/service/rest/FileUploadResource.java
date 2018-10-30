/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.service.rest;

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
import org.locationtech.geowave.service.rest.operations.RestOperationStatusMessage;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.ext.fileupload.RestletFileUpload;
import org.restlet.ext.jackson.JacksonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Post;
import org.restlet.resource.ResourceException;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.ParameterException;

import javax.ws.rs.BadRequestException;

/**
 * ServerResource to handle uploading files. Uses restlet fileupload.
 */
public class FileUploadResource extends
		ServerResource
{
	private static final Logger LOGGER = LoggerFactory.getLogger(AsyncOperationStatusResource.class);

	private static final String KEY_BATCH_UUID = "batchUUID";

	/**
	 * processes uploaded file, storing in a temporary directory
	 *
	 * @param entity
	 * @return the directory storing the uploaded file
	 * @throws Exception
	 */
	@Post
	public Representation accept(
			final Representation entity )
			throws Exception {
		RestOperationStatusMessage status = new RestOperationStatusMessage();
		try {
			if (isMediaType(
					entity,
					MediaType.MULTIPART_FORM_DATA)) {
				// 1/ Create a factory for disk-based file items
				final DiskFileItemFactory factory = new DiskFileItemFactory();
				factory.setSizeThreshold(Integer.MAX_VALUE);

				// 2/ Create a new file upload handler based on the Restlet
				// FileUpload extension that will parse Restlet requests and
				// generates FileItems.
				final RestletFileUpload upload = new RestletFileUpload(
						factory);

				final List<FileItem> fileList = upload.parseRepresentation(entity);
				if (fileList.size() != 1) {
					throw new ParameterException(
							"Operation requires exactly one file.");
				}
				FileItem item = fileList.get(0);
				// 3/ Request is parsed by the handler which generates a
				// list of FileItems
				final String tempDir = System.getProperty("java.io.tmpdir");
				// HP Fortify "Path Traversal" false positive
				// A user would need to have OS-level access anyway
				// to change the system properties
				final File dir = new File(
						tempDir);
				final File filename = File.createTempFile(
						"uploadedfile-",
						"-" + item.getName(),
						dir);
				FileUtils.copyInputStreamToFile(
						item.getInputStream(),
						filename);
				status.status = RestOperationStatusMessage.StatusType.COMPLETE;
				status.message = "File uploaded to: " + filename.getAbsolutePath();
				setStatus(Status.SUCCESS_CREATED);
			}
			else {
				throw new BadRequestException(
						"Operation only supports Multipart Form Data media type.");
			}
		}
		catch (final ParameterException e) {
			LOGGER.error(
					"Entered an error handling a request.",
					e);
			final RestOperationStatusMessage rm = new RestOperationStatusMessage();
			rm.status = RestOperationStatusMessage.StatusType.ERROR;
			rm.message = e.getMessage();
			setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
			final JacksonRepresentation<RestOperationStatusMessage> rep = new JacksonRepresentation<RestOperationStatusMessage>(
					rm);
			return rep;
		}
		catch (final BadRequestException e) {
			LOGGER.error(
					"Entered an error handling a request.",
					e);
			final RestOperationStatusMessage rm = new RestOperationStatusMessage();
			rm.status = RestOperationStatusMessage.StatusType.ERROR;
			rm.message = e.getMessage();
			setStatus(Status.CLIENT_ERROR_UNSUPPORTED_MEDIA_TYPE);
			final JacksonRepresentation<RestOperationStatusMessage> rep = new JacksonRepresentation<RestOperationStatusMessage>(
					rm);
			return rep;
		}
		catch (final Exception e) {
			LOGGER.error(
					"Entered an error handling a request.",
					e);
			final RestOperationStatusMessage rm = new RestOperationStatusMessage();
			rm.status = RestOperationStatusMessage.StatusType.ERROR;
			rm.message = "exception occurred";
			rm.data = e;
			setStatus(Status.SERVER_ERROR_INTERNAL);
			final JacksonRepresentation<RestOperationStatusMessage> rep = new JacksonRepresentation<RestOperationStatusMessage>(
					rm);
			return rep;
		}
		return new JacksonRepresentation<RestOperationStatusMessage>(
				status);
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
