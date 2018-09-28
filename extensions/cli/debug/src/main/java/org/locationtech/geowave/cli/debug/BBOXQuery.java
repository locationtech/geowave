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

import java.io.IOException;

import org.apache.commons.lang3.time.StopWatch;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.SpatialQuery;
import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.QueryOptions;
import org.locationtech.geowave.core.store.query.aggregate.CountAggregation;
import org.locationtech.geowave.core.store.query.aggregate.CountResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

@GeowaveOperation(name = "bbox", parentOperation = DebugSection.class)
@Parameters(commandDescription = "bbox query")
public class BBOXQuery extends
		AbstractGeoWaveQuery
{
	private static Logger LOGGER = LoggerFactory.getLogger(BBOXQuery.class);

	@Parameter(names = {
		"-e",
		"--east"
	}, required = true, description = "Max Longitude of BBOX")
	private Double east;

	@Parameter(names = {
		"-w",
		"--west"
	}, required = true, description = "Min Longitude of BBOX")
	private Double west;

	@Parameter(names = {
		"-n",
		"--north"
	}, required = true, description = "Max Latitude of BBOX")
	private Double north;

	@Parameter(names = {
		"-s",
		"--south"
	}, required = true, description = "Min Latitude of BBOX")
	private Double south;

	@Parameter(names = {
		"--useAggregation",
		"-agg"
	}, description = "Compute count on the server side")
	private Boolean useAggregation = Boolean.FALSE;

	private Geometry geom;

	private void getBoxGeom() {
		geom = new GeometryFactory().toGeometry(new Envelope(
				west,
				east,
				south,
				north));
	}

	@Override
	protected long runQuery(
			final GeotoolsFeatureDataAdapter adapter,
			final ByteArrayId adapterId,
			final ByteArrayId indexId,
			final DataStore dataStore,
			final boolean debug ) {
		final StopWatch stopWatch = new StopWatch();

		getBoxGeom();

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
					new SpatialQuery(
							geom))) {
				final CountResult result = ((CountResult) (it.next()));
				if (result != null) {
					count += result.getCount();
				}
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Unable to read result",
						e);
			}
		}
		else {
			stopWatch.start();

			final CloseableIterator<Object> it = dataStore.query(
					new QueryOptions(
							adapterId,
							indexId),
					new SpatialQuery(
							geom));

			stopWatch.stop();
			System.out.println("Ran BBOX query in " + stopWatch.toString());

			stopWatch.reset();
			stopWatch.start();

			while (it.hasNext()) {
				if (debug) {
					System.out.println(it.next());
				}
				else {
					it.next();
				}
				count++;
			}

			stopWatch.stop();
			System.out.println("BBOX query results iteration took " + stopWatch.toString());
		}
		return count;
	}
}
