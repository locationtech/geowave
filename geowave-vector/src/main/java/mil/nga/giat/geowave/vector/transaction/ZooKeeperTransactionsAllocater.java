package mil.nga.giat.geowave.vector.transaction;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

/** 
 * Use ZooKeeper to maintain the population of Transaction IDs. 
 * ZooKeeper's ephemeral nodes are ideal for lock management.
 * and TXID is a node.  It is locked if it has a child ephemeral node
 * called 'lock'.  If the client dies, the node is deleted, releasing the 
 * Transaction ID.  The protocol that guarantees one client locks a TX node follows
 * the recipe for locking, where the ephemeral node with the smallest sequence number wins.
 *
 */

public class ZooKeeperTransactionsAllocater implements
		Watcher,
		TransactionsAllocater
{
	private ZooKeeper zk;
	private final String clientID;
	private final String clientTXPath;
	private final String hostPort;
	private final TransactionNotification notificationRequester;
	private Set<String> lockPaths = Collections.synchronizedSet(new HashSet<String>());

	public ZooKeeperTransactionsAllocater(
			final String clientID,
			final ZooKeeper zk,
			TransactionNotification notificationRequester ) {
		super();
		this.zk = zk;
		this.hostPort = null;
		this.clientID = clientID;
		this.clientTXPath = "/" + clientID + "/tx";
		this.notificationRequester = notificationRequester;
	}

	public ZooKeeperTransactionsAllocater(
			String hostPort,
			String clientID,
			TransactionNotification notificationRequester )
			throws IOException {
		this.hostPort = hostPort;
		zk = new ZooKeeper(
				hostPort,
				5000,
				this);
		this.clientID = clientID;
		this.clientTXPath = "/" + clientID + "/tx";
		this.notificationRequester = notificationRequester;
		try {
			init();
		}
		catch (InterruptedException | KeeperException e) {
			throw new IOException(
					e);
		}
	}

	public void close()
			throws InterruptedException {
		this.zk.close();
	}

	public void releaseTransaction(
			String txID )
			throws IOException {

		try {
			for (String child : zk.getChildren(
					clientTXPath + "/" + txID,
					false)) {
				String childPath = clientTXPath + "/" + txID + "/" + child;
				// only remove paths that have associated with a return of
				// a transaction ID to the client/caller.
				// Other paths may exist temporarily during the
				// capture process.
				if (this.lockPaths.contains(childPath)) {
					try {
						zk.delete(

								childPath,
								-1);
					}
					catch (KeeperException.NoNodeException ex) {
						// someone else beat us to it
					}
					lockPaths.remove(childPath);
				}
			}
		}
		catch (KeeperException.NoNodeException | KeeperException.ConnectionLossException | KeeperException.SessionExpiredException ex) {
			// in these cases, the ephemeral nodes are removed by the server
			// if the parent txID, does not exist (odd case, then zookeeper failed to sync prior to its
			// untimely departure
		}
		catch (Exception ex) {
			throw new IOException(
					ex);
		}

	}

	public String getTransaction()
			throws IOException {
		try {
			int count = 0;
			boolean ok = false;
			while (!ok) {

				try {

					// find all tx IDs available!
					for (String childTXID : zk.getChildren(
							clientTXPath,
							false)) {
						List<String> children = zk.getChildren(
								clientTXPath + "/" + childTXID,
								false);
						// if there are already locks, continue to the next
						if (children.size() > 0) continue;

						// create a lock
						String lockPath = create(
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
						for (String lockChild : children) {
							final String lockObj = lockPath.substring(lockPath.lastIndexOf('/') + 1);
							// smallest sequence wins
							ok &= (lockChild.compareTo(lockObj) >= 0);
						}
						// found a transaction
						if (ok) {
							this.lockPaths.add(lockPath);
							return childTXID;
						}
						try {
							// delete the attempt and try another child
							zk.delete(
									lockPath,
									-1);
						}
						catch (Exception ex) {
							// may get deleted by the release, so an error can
							// be ignored
						}
					}
					ok = false;

					// not found, so create a new one AND try to win it
					String transId = UUID.randomUUID().toString();
					this.notificationRequester.transactionCreated(
							clientID,
							transId);

					// If 'create' fails, then the transaction id is 'lost'.
					//
					// NOTE: Notifying the requester after the 'create' step
					// would be worse
					// than prior to 'create'. An error from ZK does not
					// indicate if the node creation
					// worked or not.
					// Failing to report a new transaction id to the requester
					// is more damaging
					// than a lost transaction ID (which means it never gets
					// used again).
					zk.create(
							clientTXPath + "/" + transId,
							new byte[0],
							ZooDefs.Ids.OPEN_ACL_UNSAFE,
							CreateMode.PERSISTENT);
				}
				catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException ex) {
					if (hostPort != null) reconnect();
					count++;
					if (count >= 5) throw ex;
				}
			}

		}
		catch (Exception ex) {
			throw new IOException(
					ex);
		}
		return null;

	}

	@Override
	public void process(
			WatchedEvent event ) {
		// TODO Auto-generated method stub

	}

	private String create(
			String path,
			byte[] value,
			CreateMode mode )
			throws KeeperException,
			InterruptedException {
		try {
			return zk.create(
					path,
					value,
					ZooDefs.Ids.OPEN_ACL_UNSAFE,
					mode);
		}
		catch (KeeperException.NodeExistsException ex) {
			// do nothing
		}
		return path;
	}

	private void init()
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
			catch (KeeperException.NodeExistsException ex) {

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

	private void reconnect()
			throws IOException,
			KeeperException,
			InterruptedException {
		zk = new ZooKeeper(
				hostPort,
				5000,
				this);
		init();
	}
}
