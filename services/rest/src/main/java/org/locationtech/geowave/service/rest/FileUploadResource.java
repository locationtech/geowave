/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.glassfish.jersey.media.multipart.BodyPart;
import org.glassfish.jersey.media.multipart.ContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.locationtech.geowave.service.rest.operations.RestOperationStatusMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Stores an uploaded file in the temporary directory, and reports where. */
@Path("v0/fileupload")
public class FileUploadResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileUploadResource.class);

  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  public Response accept(final FormDataMultiPart multiPart) {
    try {
      final List<BodyPart> parts = multiPart.getBodyParts();
      if (parts.size() != 1) {
        return JsonResponses.of(
            Response.Status.BAD_REQUEST,
            JsonResponses.error("Operation requires exactly one file.", null));
      }
      final BodyPart part = parts.get(0);
      final ContentDisposition disposition = part.getContentDisposition();
      final File file =
          File.createTempFile(
              "uploadedfile-",
              "-" + safeFileName((disposition == null) ? null : disposition.getFileName()),
              new File(System.getProperty("java.io.tmpdir")));
      try (InputStream in = part.getEntityAs(InputStream.class)) {
        Files.copy(in, file.toPath(), StandardCopyOption.REPLACE_EXISTING);
      }
      final RestOperationStatusMessage status = new RestOperationStatusMessage();
      status.status = RestOperationStatusMessage.StatusType.COMPLETE;
      status.message = "File uploaded to: " + file.getAbsolutePath();
      return JsonResponses.of(Response.Status.CREATED, status);
    } catch (final Exception e) {
      LOGGER.error("Entered an error handling a request.", e);
      return JsonResponses.of(
          Response.Status.INTERNAL_SERVER_ERROR,
          JsonResponses.error("exception occurred", e));
    } finally {
      multiPart.cleanup();
    }
  }

  /** The client's file name, reduced to one path segment of safe characters. */
  static String safeFileName(final String fileName) {
    if (fileName == null) {
      return "upload";
    }
    final String name =
        fileName.substring(Math.max(fileName.lastIndexOf('/'), fileName.lastIndexOf('\\')) + 1);
    return name.isEmpty() ? "upload" : name.replaceAll("[^A-Za-z0-9._-]", "_");
  }
}
