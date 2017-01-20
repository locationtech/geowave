package mil.nga.giat.geowave.adapter.vector.plugin.transaction;

import java.io.IOException;
import java.util.LinkedList;
import java.util.UUID;

public class MemoryTransactionsAllocator implements
		TransactionsAllocator
{
	private final LinkedList<String> lockPaths = new LinkedList<String>();

	public MemoryTransactionsAllocator() {
		super();
	}

	public void close()
			throws InterruptedException {}

	@Override
	public void releaseTransaction(
			final String txID )
			throws IOException {
		synchronized (lockPaths) {
			if (!lockPaths.contains(txID)) {
				lockPaths.add(txID);
			}
		}

	}

	@Override
	public String getTransaction()
			throws IOException {
		synchronized (lockPaths) {
			if (lockPaths.size() > 0) {
				return lockPaths.removeFirst();
			}
		}
		return UUID.randomUUID().toString();
	}

}