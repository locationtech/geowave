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
package mil.nga.giat.geowave.cli.debug;

import java.io.IOException;

import org.geotools.filter.text.cql2.CQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.query.aggregate.CountAggregation;
import mil.nga.giat.geowave.core.store.query.aggregate.CountResult;

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
	private Boolean useAggregation = Boolean.FALSE;

	@Override
	protected long runQuery(
			final GeotoolsFeatureDataAdapter adapter,
			final ByteArrayId adapterId,
			final ByteArrayId indexId,
			final DataStore dataStore,
			final boolean debug ) {
		long count = 0;
		if (useAggregation) {
			final QueryOptions options = new QueryOptions(
					adapterId,
					indexId);
			options.setAggregation(
					new CountAggregation(),
					adapter);
			try (final CloseableIterator<Object> it = dataStore.query(
					options,
					mil.nga.giat.geowave.adapter.vector.query.cql.CQLQuery.createOptimalQuery(
							cqlStr,
							adapter,
							null,
							null))) {
				if (it.hasNext()) {
					final CountResult result = ((CountResult) (it.next()));
					if (result != null) {
						count += result.getCount();
					}
				}
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Unable to read result",
						e);
			}
			catch (final CQLException e1) {
				LOGGER.error(
						"Unable to create optimal query",
						e1);
			}
			return count;
		}
		else {
			try (final CloseableIterator<Object> it = dataStore.query(
					new QueryOptions(
							adapterId,
							indexId),
					mil.nga.giat.geowave.adapter.vector.query.cql.CQLQuery.createOptimalQuery(
							cqlStr,
							adapter,
							null,
							null))) {
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
			catch (final IOException e) {
				LOGGER.warn(
						"Unable to read result",
						e);
			}
			catch (final CQLException e1) {
				LOGGER.error(
						"Unable to create optimal query",
						e1);
			}
			return count;
		}
	}
}
