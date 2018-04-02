package mil.nga.giat.geowave.core.store;

public abstract class BaseStoreFactory<T> implements
		GenericStoreFactory<T>
{
	private final String typeName;
	private final String description;
	protected StoreFactoryHelper helper;

	public BaseStoreFactory(
			final String typeName,
			final String description,
			final StoreFactoryHelper helper ) {
		super();
		this.typeName = typeName;
		this.description = description;
		this.helper = helper;
	}

	@Override
	public String getType() {
		return typeName;
	}

	@Override
	public String getDescription() {
		return description;
	}

	@Override
	public StoreFactoryOptions createOptionsInstance() {
		return helper.createOptionsInstance();
	}

}
