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
package mil.nga.giat.geowave.core.index;

import java.util.Arrays;

/**
 * This interface defines a multi-tiered approach to indexing, in which a single
 * strategy is reliant on a set of sub-strategies
 * 
 */
public interface HierarchicalNumericIndexStrategy extends
		NumericIndexStrategy
{
	public SubStrategy[] getSubStrategies();

	public static class SubStrategy
	{
		private final NumericIndexStrategy indexStrategy;
		private final byte[] prefix;

		public SubStrategy(
				final NumericIndexStrategy indexStrategy,
				final byte[] prefix ) {
			this.indexStrategy = indexStrategy;
			this.prefix = prefix;
		}

		public NumericIndexStrategy getIndexStrategy() {
			return indexStrategy;
		}

		public byte[] getPrefix() {
			return prefix;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((indexStrategy == null) ? 0 : indexStrategy.hashCode());
			result = prime * result + Arrays.hashCode(prefix);
			return result;
		}

		@Override
		public boolean equals(
				Object obj ) {
			if (this == obj) return true;
			if (obj == null) return false;
			if (getClass() != obj.getClass()) return false;
			SubStrategy other = (SubStrategy) obj;
			if (indexStrategy == null) {
				if (other.indexStrategy != null) return false;
			}
			else if (!indexStrategy.equals(other.indexStrategy)) return false;
			if (!Arrays.equals(
					prefix,
					other.prefix)) return false;
			return true;
		}
	}
}
