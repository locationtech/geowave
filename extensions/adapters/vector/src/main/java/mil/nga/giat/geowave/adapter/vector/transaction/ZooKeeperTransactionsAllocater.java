package mil.nga.giat.geowave.adapter.vector.transaction;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;

/**
 * Use ZooKeeper to maintain the population of Transaction IDs. ZooKeeper's
 * ephemeral nodes are ideal for lock management. and TXID is a node. It is
 * locked if it has a child ephemeral node called 'lock'. If the client dies,
 * the node is deleted, releasing the Transaction ID. The protocol that
 * guarantees one client locks a TX node follows the recipe for locking, where
 * the ephemeral node with the smallest sequence number wins.
 * 
 */

public class ZooKeeperTransactionsAllocater implements
		Watcher,
		TransactionsAllocater
{

	private final static Logger LOGGER = Logger.getLogger(ZooKeeperTransactionsAllocater.class);
	private ZooKeeper zk;
	private final String clientID;
	private final String clientTXPath;
	private final String hostPort;
	private final TransactionNotification notificationRequester;
	private boolean autoAllocateNewTransactionID = true;
	private final Set<String> lockPaths = Collections.synchronizedSet(new HashSet<String>());

	private enum MyState {
		CONNECTING,
		CONNECTED,
		IDLE
	};

	private MyState myState = MyState.IDLE;

	public ZooKeeperTransactionsAllocater(
			final String clientID,
			final ZooKeeper zk,
			final TransactionNotification notificationRequester ) {
		super();
		this.zk = zk;
		hostPort = null;
		this.clientID = clientID;
		clientTXPath = "/" + clientID + "/tx";
		this.notificationRequester = notificationRequester;
	}

	public ZooKeeperTransactionsAllocater(
			final String hostPort,
			final String clientID,
			final TransactionNotification notificationRequester )
			throws IOException {
		this.hostPort = hostPort;
		this.clientID = clientID;
		this.clientTXPath = "/" + clientID + "/tx";
		this.notificationRequester = notificationRequester;
		try {
			reconnect();
		}
		catch (InterruptedException | KeeperException e) {
			throw new IOException(
					e);
		}
	}

	public void close()
			throws InterruptedException {
		zk.close();
	}

	@Override
	public void releaseTransaction(
			final String txID )
			throws IOException {

		try {
			for (final String child : zk.getChildren(
					clientTXPath + "/" + txID,
					false)) {
				final String childPath = clientTXPath + "/" + txID + "/" + child;
				// only remove paths that have associated with a return of
				// a transaction ID to the client/caller.
				// Other paths may exist temporarily during the
				// capture process.
				if (lockPaths.contains(childPath)) {
					try {
						zk.delete(

								childPath,
								-1);
					}
					catch (final KeeperException.NoNodeException ex) {
						// someone else beat us to it
					}
					lockPaths.remove(childPath);
				}
			}
		}
		catch (KeeperException.NoNodeException | KeeperException.ConnectionLossException | KeeperException.SessionExpiredException ex) {
			// in these cases, the ephemeral nodes are removed by the server
			// if the parent txID, does not exist (odd case, then zookeeper
			// failed to sync prior to its
			// untimely departure
		}
		catch (final Exception ex) {
			LOGGER.error(
					"Exception releasing transaction",
					ex);
		}

	}

	/**
	 * Pre-allocate transaction IDs using an administrative account for another
	 * account
	 * 
	 * @param maximumAmount
	 *            maximum number of transaction IDs needed
	 * @param userAccount
	 *            the user account to use those IDs
	 */
	public void preallocateTransactionIDs(
			final int maximumAmount,
			String userAccount ) {
		try {

			final String localClientTXPath = "/" + userAccount + "/tx";
			init(
					userAccount,
					localClientTXPath);
			final List<String> children = zk.getChildren(
					localClientTXPath,
					false);
			final int amountToAdd = maximumAmount - children.size();
			for (int i = 0; i < amountToAdd; i++) {
				final String transId = UUID.randomUUID().toString();
				if (notificationRequester.transactionCreated(
						userAccount,
						transId)) {
					zk.create(
							localClientTXPath + "/" + transId,
							new byte[0],
							ZooDefs.Ids.OPEN_ACL_UNSAFE,
							CreateMode.PERSISTENT);
				}
			}
			LOGGER.info("Added " + amountToAdd + " useable transaction IDs");
		}
		catch (final Exception ex) {
			LOGGER.error(
					"Failed to preallocate a useable transaction ID",
					ex);
		}
	}

	@Override
	public String getTransaction()
			throws IOException {
		try {
			int count = 0;
			boolean ok = false;
			while (!ok) {

				try {

					// find all tx IDs available!
					for (final String childTXID : zk.getChildren(
							clientTXPath,
							false)) {
						List<String> children = zk.getChildren(
								clientTXPath + "/" + childTXID,
								false);
						// if there are already locks, continue to the next
						if (children.size() > 0) {
							continue;
						}

						// create a lock
						final String lockPath = create(
								clientTXPath + "/" + childTXID + "/lock",
								new byte[] {
									0x01
								},
								CreateMode.EPHEMERAL_SEQUENTIAL);

						// now, double check that this client got the 'first'
						// one
						children = zk.getChildren(
								clientTXPath + "/" + childTXID,
								false);

						ok = true;
						for (final String lockChild : children) {
							final String lockObj = lockPath.substring(lockPath.lastIndexOf('/') + 1);
							// smallest sequence wins
							ok &= (lockChild.compareTo(lockObj) >= 0);
						}
						// found a transaction
						if (ok) {
							lockPaths.add(lockPath);
							return childTXID;
						}
						try {
							// delete the attempt and try another child
							zk.delete(
									lockPath,
									-1);
						}
						catch (final Exception ex) {
							// may get deleted by the release, so an error can
							// be ignored
						}
					}
					ok = false;

					// not found, so create a new one AND try to win it
					final String transId = UUID.randomUUID().toString();
					if (autoAllocateNewTransactionID && notificationRequester.transactionCreated(
							clientID,
							transId)) {

						// If 'create' fails, then the transaction id is 'lost'.
						//
						// NOTE: Notifying the requester after the 'create' step
						// would be worse
						// than prior to 'create'. An error from ZK does not
						// indicate if the node creation
						// worked or not.
						// Failing to report a new transaction id to the
						// requester
						// is more damaging
						// than a lost transaction ID (which means it never gets
						// used again).
						zk.create(
								clientTXPath + "/" + transId,
								new byte[0],
								ZooDefs.Ids.OPEN_ACL_UNSAFE,
								CreateMode.PERSISTENT);
					}
					else if (autoAllocateNewTransactionID) {
						autoAllocateNewTransactionID = false;
					}
				}
				catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException | java.nio.channels.CancelledKeyException ex) {
					if (hostPort != null) {
						reconnect();
					}
					count++;
					if (count > 5) {
						throw ex;
					}
				}
			}

		}
		catch (final Exception ex) {
			LOGGER.error(ex);
			throw new IOException(
					ex);
		}
		return null;

	}

	@Override
	public synchronized void process(
			final WatchedEvent event ) {
		if (event.getState() == KeeperState.AuthFailed) {
			LOGGER.error("Failed to authenticate with Zooker");
			myState = MyState.IDLE;
		}
		else if (event.getState() == KeeperState.SyncConnected && myState != MyState.CONNECTED) {
			myState = MyState.CONNECTED;
			this.notifyAll();
		}
	}

	private String create(
			final String path,
			final byte[] value,
			final CreateMode mode )
			throws KeeperException,
			InterruptedException {
		try {
			return zk.create(
					path,
					value,
					ZooDefs.Ids.OPEN_ACL_UNSAFE,
					mode);
		}
		catch (final KeeperException.NodeExistsException ex) {
			// do nothing
		}
		return path;
	}

	protected ZooKeeper getConnection() {
		return zk;
	}

	private void init(
			final String clientID,
			final String clientTXPath )
			throws KeeperException,
			InterruptedException {
		if (zk.exists(
				"/" + clientID,
				false) == null) {
			try {
				create(
						"/" + clientID,
						new byte[0],
						CreateMode.PERSISTENT);
			}
			catch (final KeeperException.NodeExistsException ex) {

			}
		}
		if (zk.exists(
				clientTXPath,
				false) == null) {
			create(
					clientTXPath,
					new byte[0],
					CreateMode.PERSISTENT);
		}

	}

	private synchronized void reconnect()
			throws IOException,
			KeeperException,
			InterruptedException {
		LOGGER.info("Connecting to Zookeeper");
		boolean doInit = false;
		// only one client class should try to perform the connection
		if (myState != MyState.CONNECTING) {
			myState = MyState.CONNECTING;
			doInit = true;
			zk = new ZooKeeper(
					hostPort,
					15000,
					this);
		}

		while (zk.getState() == States.CONNECTING || zk.getState() == States.NOT_CONNECTED)
			this.wait();
		if (doInit) init(
				clientID,
				clientTXPath);

	}

}
