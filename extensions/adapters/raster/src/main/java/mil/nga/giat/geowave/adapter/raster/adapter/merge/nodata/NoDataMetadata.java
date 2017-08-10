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
package mil.nga.giat.geowave.adapter.raster.adapter.merge.nodata;

import java.util.Set;

import mil.nga.giat.geowave.core.index.persist.Persistable;

public interface NoDataMetadata extends
		Persistable
{
	public static class SampleIndex
	{
		private final int x;
		private final int y;
		private final int b;

		public SampleIndex(
				final int x,
				final int y,
				final int b ) {
			this.x = x;
			this.y = y;
			this.b = b;
		}

		public int getX() {
			return x;
		}

		public int getY() {
			return y;
		}

		public int getBand() {
			return b;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + b;
			result = prime * result + x;
			result = prime * result + y;
			return result;
		}

		@Override
		public boolean equals(
				Object obj ) {
			if (this == obj) return true;
			if (obj == null) return false;
			if (getClass() != obj.getClass()) return false;
			SampleIndex other = (SampleIndex) obj;
			if (b != other.b) return false;
			if (x != other.x) return false;
			if (y != other.y) return false;
			return true;
		}
	}

	public boolean isNoData(
			SampleIndex index,
			double sampleValue );

	public Set<SampleIndex> getNoDataIndices();

}
