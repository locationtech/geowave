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
package org.locationtech.geowave.cli.debug;

import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.api.VectorAggregationQueryBuilder;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataStore;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "serverCql", parentOperation = DebugSection.class)
@Parameters(commandDescription = "cql server-side")
public class CQLQuery extends
		AbstractGeoWaveQuery
{
	private static Logger LOGGER = LoggerFactory.getLogger(CQLQuery.class);

	@Parameter(names = "--cql", required = true, description = "CQL Filter executed client side")
	private String cqlStr;

	@Parameter(names = {
		"--useAggregation",
		"-agg"
	}, description = "Compute count on the server side")
	private final Boolean useAggregation = Boolean.FALSE;

	@Override
	protected long runQuery(
			final GeotoolsFeatureDataAdapter adapter,
			final String typeName,
			final String indexName,
			final DataStore dataStore,
			final boolean debug ) {
		long count = 0;
		if (useAggregation) {
			final VectorAggregationQueryBuilder<Persistable, Long> bldr = (VectorAggregationQueryBuilder) VectorAggregationQueryBuilder
					.newBuilder()
					.count(
							typeName)
					.indexName(
							indexName);
			final Long countResult = dataStore.aggregate(bldr.constraints(
					bldr.constraintsFactory().cqlConstraints(
							cqlStr)).build());
			if (countResult != null) {
				count += countResult;
			}
			return count;
		}
		else {
			final VectorQueryBuilder bldr = VectorQueryBuilder.newBuilder().addTypeName(
					typeName).indexName(
					indexName);

			try (final CloseableIterator<SimpleFeature> it = dataStore.query(bldr.constraints(
					bldr.constraintsFactory().cqlConstraints(
							cqlStr)).build())) {
				while (it.hasNext()) {
					if (debug) {
						System.out.println(it.next());
					}
					else {
						it.next();
					}
					count++;
				}
			}
			return count;
		}
	}
}
