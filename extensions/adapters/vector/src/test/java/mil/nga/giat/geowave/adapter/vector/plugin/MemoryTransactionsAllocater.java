package mil.nga.giat.geowave.adapter.vector.plugin;

import java.io.IOException;
import java.util.LinkedList;
import java.util.UUID;

import mil.nga.giat.geowave.adapter.vector.transaction.TransactionNotification;
import mil.nga.giat.geowave.adapter.vector.transaction.TransactionsAllocater;

public class MemoryTransactionsAllocater implements
		TransactionsAllocater
{

	private TransactionNotification notificationRequester;
	private String userId;

	private final LinkedList<String> lockPaths = new LinkedList<String>();

	public MemoryTransactionsAllocater(
			String userId ) {
		super();
		this.userId = userId;
	}

	public MemoryTransactionsAllocater(
			TransactionNotification notificationRequester ) {
		super();
		this.notificationRequester = notificationRequester;
	}

	public void setNotificationRequester(
			TransactionNotification notificationRequester ) {
		this.notificationRequester = notificationRequester;
	}

	public TransactionNotification getNotificationRequester() {
		return notificationRequester;
	}

	public void close()
			throws InterruptedException {}

	public void releaseTransaction(
			String txID )
			throws IOException {
		synchronized (lockPaths) {
			if (!lockPaths.contains(txID)) lockPaths.add(txID);
		}

	}

	public String getTransaction()
			throws IOException {
		synchronized (lockPaths) {
			if (lockPaths.size() > 0) {
				return lockPaths.removeFirst();
			}
		}
		String id = UUID.randomUUID().toString();
		if (notificationRequester != null) notificationRequester.transactionCreated(
				userId,
				id);
		return id;
	}

}
