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
package org.locationtech.geowave.analytic.mapreduce.dbscan;

import java.util.Set;

import org.locationtech.geowave.analytic.nn.NeighborList;
import org.locationtech.geowave.core.index.ByteArray;

import com.vividsolutions.jts.geom.Geometry;

public interface Cluster extends
		NeighborList<ClusterItem>
{
	public void merge(
			Cluster cluster );

	public ByteArray getId();

	/*
	 * Return the cluster to which this cluster is linked
	 */
	public Set<ByteArray> getLinkedClusters();

	public int currentLinkSetSize();

	public void invalidate();

	public void finish();

	public boolean isCompressed();

	public Geometry getGeometry();

}
