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
package mil.nga.giat.geowave.analytic.mapreduce.dbscan;

import com.vividsolutions.jts.geom.Geometry;

/**
 * A DB Scan cluster Item
 */
public class ClusterItem
{
	private final String id;
	private Geometry geometry;
	private long count;
	private boolean compressed = false;

	public ClusterItem(
			final String id,
			final Geometry geometry,
			final long count,
			final boolean compressed ) {
		super();
		this.id = id;
		this.geometry = geometry;
		this.count = count;
		this.compressed = compressed;
	}

	public void setCompressed() {
		this.compressed = true;
	}

	protected boolean isCompressed() {
		return this.compressed;
	}

	protected String getId() {
		return id;
	}

	protected Geometry getGeometry() {
		return geometry;
	}

	protected long getCount() {
		return count;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		ClusterItem other = (ClusterItem) obj;
		if (id == null) {
			if (other.id != null) return false;
		}
		else if (!id.equals(other.id)) return false;
		return true;
	}

	@Override
	public String toString() {
		return "ClusterItem [id=" + id + ", geometry=" + geometry + ", count=" + count + "]";
	}

	public void setGeometry(
			Geometry geometry ) {
		this.geometry = geometry;
	}

	public void setCount(
			long count ) {
		this.count = count;
	}

}
