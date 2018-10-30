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
package org.locationtech.geowave.service.client;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.proxy.WebResourceFactory;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
import org.locationtech.geowave.service.FileUploadService;

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
