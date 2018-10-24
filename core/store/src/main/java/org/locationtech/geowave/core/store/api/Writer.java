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
package org.locationtech.geowave.core.store.api;

import java.io.Closeable;

import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.store.data.VisibilityWriter;

public interface Writer<T> extends
		Closeable
{
	/**
	 * Write the entry using any basic visibilities set elsewhere
	 *
	 * @param entry
	 *            the entry to write
	 * @return the Insertion IDs representing where this entry was written
	 */
	InsertionIds write(
			final T entry );

	/**
	 * write the entry using visibilities determined by the
	 * fieldVisibilityyWriter
	 *
	 * @param entry
	 *            the entry
	 * @param fieldVisibilityWriter
	 *            method for determining visibility per field
	 * @return the Insertion IDs representing where this entry was written
	 */
	InsertionIds write(
			final T entry,
			final VisibilityWriter<T> fieldVisibilityWriter );

	/**
	 * get the indices that are being written to
	 *
	 * @return the indices that are being written to
	 */
	Index[] getIndices();

	/**
	 * flush the underlying row writer to ensure entries queued for write are
	 * fully written. This is particularly useful for streaming data as an
	 * intermittent mechanism to ensure periodic updates are being stored.
	 */
	void flush();

	/**
	 * flush all entries enqueued and close all resources for this writer
	 *
	 */
	@Override
	void close();
}
