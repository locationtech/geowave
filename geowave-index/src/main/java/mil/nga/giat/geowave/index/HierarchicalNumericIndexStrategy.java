package mil.nga.giat.geowave.index;

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
	}
}
