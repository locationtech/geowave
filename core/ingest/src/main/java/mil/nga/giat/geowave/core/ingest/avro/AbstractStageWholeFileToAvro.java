/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.core.ingest.avro;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.file.Files;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class can be sub-classed as a general-purpose recipe for parallelizing
 * ingestion of files by directly staging the binary of the file to Avro.
 */
abstract public class AbstractStageWholeFileToAvro<O> implements
		AvroFormatPlugin<WholeFile, O>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AbstractStageWholeFileToAvro.class);

	@Override
	public Schema getAvroSchema() {
		return WholeFile.getClassSchema();
	}

	@Override
	public WholeFile[] toAvroObjects(
			final URL f ) {
		try {
			// TODO: consider a streaming mechanism in case a single file is too
			// large
			return new WholeFile[] {
				new WholeFile(
						ByteBuffer.wrap(IOUtils.toByteArray(f)),
						f.getPath())
			};
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to read file",
					e);
		}
		return new WholeFile[] {};

	}

}
