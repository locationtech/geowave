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

import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "clientCql", parentOperation = DebugSection.class)
@Parameters(commandDescription = "cql client-side, primarily useful for consistency checking")
public class ClientSideCQLQuery extends
		AbstractGeoWaveQuery
{
	private static Logger LOGGER = LoggerFactory.getLogger(ClientSideCQLQuery.class);

	@Parameter(names = "--cql", required = true, description = "CQL Filter executed client side")
	private String cql;

	private Filter filter;

	private void getFilter() {
		try {
			filter = ECQL.toFilter(cql);
		}
		catch (final CQLException e) {
			LOGGER.warn(
					"Unable to retrive filter",
					e);
		}
	}

	@Override
	protected long runQuery(
			final GeotoolsFeatureDataAdapter adapter,
			final String typeName,
			final String indexName,
			final DataStore dataStore,
			final boolean debug ) {
		getFilter();

		long count = 0;
		try (final CloseableIterator<Object> it = dataStore.query(QueryBuilder.newBuilder().addTypeName(
				typeName).indexName(
				indexName).build())) {
			while (it.hasNext()) {
				final Object o = it.next();
				if (o instanceof SimpleFeature) {
					if (filter.evaluate(o)) {
						if (debug) {
							System.out.println(o);
						}
						count++;
					}
				}
			}
		}
		return count;
	}
}
