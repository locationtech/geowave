package mil.nga.giat.geowave.vector.transaction;

public interface TransactionNotification
{
	public void transactionCreated(String clientID, String txID);
}
