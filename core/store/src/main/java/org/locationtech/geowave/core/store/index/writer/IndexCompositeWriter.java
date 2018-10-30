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
package org.locationtech.geowave.core.store.index.writer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.SinglePartitionInsertionIds;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.data.VisibilityWriter;

public class IndexCompositeWriter<T> implements
		Writer<T>
{
	final Writer<T>[] writers;

	public IndexCompositeWriter(
			final Writer<T>[] writers ) {
		super();
		this.writers = writers;
	}

	@Override
	public void close() {
		for (final Writer<T> indexWriter : writers) {
			indexWriter.close();
		}
	}

	@Override
	public InsertionIds write(
			final T entry ) {
		final List<SinglePartitionInsertionIds> ids = new ArrayList<>();

		for (final Writer<T> indexWriter : writers) {
			final InsertionIds i = indexWriter.write(entry);
			ids.addAll(i.getPartitionKeys());
		}
		return new InsertionIds(
				ids);
	}

	@Override
	public InsertionIds write(
			final T entry,
			final VisibilityWriter<T> fieldVisibilityWriter ) {
		final List<SinglePartitionInsertionIds> ids = new ArrayList<>();
		for (final Writer<T> indexWriter : writers) {
			final InsertionIds i = indexWriter.write(
					entry,
					fieldVisibilityWriter);
			ids.addAll(i.getPartitionKeys());
		}
		return new InsertionIds(
				ids);
	}

	@Override
	public Index[] getIndices() {
		final List<Index> ids = new ArrayList<>();
		for (final Writer<T> indexWriter : writers) {
			ids.addAll(Arrays.asList(indexWriter.getIndices()));
		}
		return ids.toArray(new Index[ids.size()]);
	}

	@Override
	public void flush() {
		for (final Writer<T> indexWriter : writers) {
			indexWriter.flush();
		}
	}

}
