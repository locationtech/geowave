/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.ingest.hdfs.mapreduce;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.ingest.IngestPluginBase;

/**
 * This interface is used by the IngestFromHdfsPlugin to implement ingestion within a mapper only.
 * The implementation will be directly persisted to a mapper and called to produce GeoWaveData to be
 * written.
 *
 * @param <I> data type for intermediate data
 * @param <O> data type that will be ingested into GeoWave
 */
public interface IngestWithMapper<I, O> extends IngestPluginBase<I, O>, Persistable {
}
