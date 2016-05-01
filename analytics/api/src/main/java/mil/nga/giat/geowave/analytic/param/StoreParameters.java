package mil.nga.giat.geowave.analytic.param;

public class StoreParameters
{
	public enum StoreParam
			implements
			ParameterEnum {
		INPUT_STORE(
				new InputStoreParameterHelper()),
		OUTPUT_STORE(
				new OutputStoreParameterHelper()), ;

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
