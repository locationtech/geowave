package mil.nga.giat.geowave.adapter.vector.transaction;

import java.io.IOException;

/**
 * 
 * Allocate a transaction ID. Controls the space of transaction IDs, allowing
 * them to be reusable. Essentially represents an unbounded pool of IDs.
 * However, upper bound is determined by the number of simultaneous
 * transactions.
 * 
 * The set of IDs is associated with visibility/access.
 * 
 */
public interface TransactionsAllocater
{
	public String getTransaction()
			throws IOException;

	public void releaseTransaction(
			String txID )
			throws IOException;

}
