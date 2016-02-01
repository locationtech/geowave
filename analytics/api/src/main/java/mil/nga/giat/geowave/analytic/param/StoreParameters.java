package mil.nga.giat.geowave.analytic.param;

public class StoreParameters
{
	public enum StoreParam
			implements
			ParameterEnum {
		DATA_STORE(
				new DataStoreParameterHelper()),
		ADAPTER_STORE(
				new AdapterStoreParameterHelper()),
		INDEX_STORE(
				new IndexStoreParameterHelper()),
		DATA_STATISTICS_STORE(
				new DataStatisticsStoreParameterHelper());

		private final ParameterHelper<?> helper;

		private StoreParam(
				final ParameterHelper<?> helper ) {
			this.helper = helper;
		}

		@Override
		public Enum<?> self() {
			return this;
		}

		@Override
		public ParameterHelper<?> getHelper() {
			return helper;
		}
	}
}
