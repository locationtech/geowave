package mil.nga.giat.geowave.vector.transaction;

public interface TransactionNotification
{
	public boolean transactionCreated(
			String clientID,
			String txID );
}
