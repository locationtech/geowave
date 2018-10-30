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
package org.locationtech.geowave.datastore.cassandra.operations;

import java.nio.ByteBuffer;

import org.bouncycastle.util.Arrays;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.util.StatisticsRowIterator;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.google.common.collect.Iterators;

public class CassandraMetadataReader implements
		MetadataReader
{
	private final CassandraOperations operations;
	private final MetadataType metadataType;

	public CassandraMetadataReader(
			final CassandraOperations operations,
			final MetadataType metadataType ) {
		this.operations = operations;
		this.metadataType = metadataType;
	}

	@Override
	public CloseableIterator<GeoWaveMetadata> query(
			final MetadataQuery query ) {
		final String tableName = operations.getMetadataTableName(metadataType);
		String[] selectedColumns = getSelectedColumns(query);
		if (MetadataType.STATS.equals(metadataType)) {
			selectedColumns = Arrays.append(
					selectedColumns,
					CassandraMetadataWriter.VISIBILITY_KEY);
		}
		final Select select = operations.getSelect(
				tableName,
				selectedColumns);
		if (query.hasPrimaryId()) {
			final Where where = select.where(QueryBuilder.eq(
					CassandraMetadataWriter.PRIMARY_ID_KEY,
					ByteBuffer.wrap(query.getPrimaryId())));
			if (query.hasSecondaryId()) {
				where.and(QueryBuilder.eq(
						CassandraMetadataWriter.SECONDARY_ID_KEY,
						ByteBuffer.wrap(query.getSecondaryId())));
			}
		}
		else if (query.hasSecondaryId()) {
			select.allowFiltering().where(
					QueryBuilder.eq(
							CassandraMetadataWriter.SECONDARY_ID_KEY,
							ByteBuffer.wrap(query.getSecondaryId())));
		}
		final ResultSet rs = operations.getSession().execute(
				select);
		final CloseableIterator<GeoWaveMetadata> retVal = new CloseableIterator.Wrapper<>(
				Iterators.transform(
						rs.iterator(),
						new com.google.common.base.Function<Row, GeoWaveMetadata>() {
							@Override
							public GeoWaveMetadata apply(
									final Row result ) {
								return new GeoWaveMetadata(
										query.hasPrimaryId() ? query.getPrimaryId() : result.get(
												CassandraMetadataWriter.PRIMARY_ID_KEY,
												ByteBuffer.class).array(),
										useSecondaryId(query) ? query.getSecondaryId() : result.get(
												CassandraMetadataWriter.SECONDARY_ID_KEY,
												ByteBuffer.class).array(),
										getVisibility(result),
										result.get(
												CassandraMetadataWriter.VALUE_KEY,
												ByteBuffer.class).array());
							}
						}));
		return MetadataType.STATS.equals(metadataType) ? new StatisticsRowIterator(
				retVal,
				query.getAuthorizations()) : retVal;
	}

	private byte[] getVisibility(
			Row result ) {
		if (MetadataType.STATS.equals(metadataType)) {
			ByteBuffer buf = result.get(
					CassandraMetadataWriter.VISIBILITY_KEY,
					ByteBuffer.class);
			if (buf != null) {
				return buf.array();
			}
		}
		return null;
	}

	private String[] getSelectedColumns(
			final MetadataQuery query ) {
		if (query.hasPrimaryId()) {
			if (useSecondaryId(query)) {
				return new String[] {
					CassandraMetadataWriter.VALUE_KEY
				};
			}

			return new String[] {
				CassandraMetadataWriter.SECONDARY_ID_KEY,
				CassandraMetadataWriter.VALUE_KEY
			};
		}
		if (useSecondaryId(query)) {
			return new String[] {
				CassandraMetadataWriter.PRIMARY_ID_KEY,
				CassandraMetadataWriter.VALUE_KEY
			};
		}
		return new String[] {
			CassandraMetadataWriter.PRIMARY_ID_KEY,
			CassandraMetadataWriter.SECONDARY_ID_KEY,
			CassandraMetadataWriter.VALUE_KEY
		};
	}

	private boolean useSecondaryId(
			final MetadataQuery query ) {
		return !MetadataType.STATS.equals(metadataType) || query.hasSecondaryId();
	}
}
