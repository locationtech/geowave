package mil.nga.giat.geowave.adapter.vector.transaction;

public interface TransactionNotification
{
	public boolean transactionCreated(
			String clientID,
			String txID );
}
