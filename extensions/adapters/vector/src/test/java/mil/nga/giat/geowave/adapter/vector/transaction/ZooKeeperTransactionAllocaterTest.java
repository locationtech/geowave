package mil.nga.giat.geowave.adapter.vector.transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import mil.nga.giat.geowave.adapter.vector.transaction.TransactionNotification;
import mil.nga.giat.geowave.adapter.vector.transaction.ZooKeeperTransactionsAllocater;

import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ZooKeeperTransactionAllocaterTest
{
	TestingServer zkTestServer;
	ZooKeeperTransactionsAllocater allocater;

	final List<String> createdTXIds = new ArrayList<String>();
	final List<Throwable> failures = new ArrayList<Throwable>();
	final Set<String> activeTX = new HashSet<String>();
	volatile boolean shutdown = false;
	private Random random = new Random();
	private int maxSize = 9;

	@Before
	public void startZookeeper()
			throws Exception {
		zkTestServer = new TestingServer(
				12181);
		allocater = new ZooKeeperTransactionsAllocater(
				zkTestServer.getConnectString(),
				"me",
				new TransactionNotification() {

					@Override
					public boolean transactionCreated(
							String clientID,
							String txID ) {
						synchronized (createdTXIds) {
							if (createdTXIds.size() == maxSize) return false;
							createdTXIds.add(txID);

							return true;
						}
					}

				});
	}

	private int runTest(
			final boolean recovery )
			throws InterruptedException {
		Thread[] thr = new Thread[10];
		for (int i = 0; i < thr.length; i++) {
			thr[i] = new Thread(
					new TXRequester(
							recovery));
			thr[i].start();
		}

		for (int i = 0; i < thr.length; i++) {
			thr[i].join();
		}
		for (Throwable error : failures) {
			error.printStackTrace();
		}
		assertEquals(
				0,
				failures.size());
		assertEquals(
				0,
				activeTX.size());
		return thr.length;
	}

	@Test
	public void test()
			throws InterruptedException {

		int workDone = runTest(false);
		System.out.println("Total created transactionIDS " + createdTXIds.size());
		assertTrue(createdTXIds.size() <= workDone);

	}

	@Test
	public void recoveryTest()
			throws InterruptedException {

		Thread thr = new Thread(
				new Runnable() {

					@Override
					public void run() {

						boolean ok = false;
						while (!ok) {
							ok = true;

							try {
								// wait for some activity before closing the
								// session to test recovery
								synchronized (activeTX) {
									activeTX.wait();
								}
								// Per hint from ZooKeeper pages, simulate a
								// session timeout by attaching an instance
								// to the same session and then closing it.
								final Object Lock = new Long(
										122);
								final ZooKeeper kp = new ZooKeeper(
										zkTestServer.getConnectString(),
										5000,
										new Watcher() {
											@Override
											public void process(
													WatchedEvent event ) {
												if (event.getState() == KeeperState.SyncConnected) {
													synchronized (Lock) {
														Lock.notify();
													}
												}
											}
										},
										allocater.getConnection().getSessionId(),
										allocater.getConnection().getSessionPasswd());
								// do not close until the connection is
								// established
								synchronized (Lock) {
									if (kp.getState() == States.CONNECTING) {
										synchronized (Lock) {
											Lock.wait();
										}
									}
									kp.close();
								}
							}
							catch (Exception e) {
								ok = false;
								e.printStackTrace();
							}
						}
					}
				});
		thr.start();
		runTest(true);
		thr.join();
	}

	private class TXRequester implements
			Runnable
	{
		final boolean recovery;

		private TXRequester(
				final boolean recovery ) {
			this.recovery = recovery;
		}

		int s = 0;

		@Override
		public void run() {
			while (s < 50 && !shutdown) {
				s++;
				try {
					Thread.sleep(100);
				}
				catch (InterruptedException e) {}
				try {
					String txID = allocater.getTransaction();
					synchronized (activeTX) {
						// no guarantees with forced session close as tested in
						// the recovery test
						assert (recovery || !activeTX.contains(txID)); // throws
																		// assertion
						// error
						activeTX.add(txID);
						activeTX.notifyAll();
					}
					try {
						Thread.sleep(200 + (Math.abs(random.nextInt()) % 200));
					}
					catch (InterruptedException e) {}
					synchronized (activeTX) {
						activeTX.remove(txID);
					}
					allocater.releaseTransaction(txID);
				}
				catch (Throwable e) {
					synchronized (failures) {
						failures.add(e);
						shutdown = true;
					}
				}

			}
		}
	}

	@Test
	public void testPreallocate()
			throws IOException {
		final List<String> precreatedTXIds = new ArrayList<String>();
		ZooKeeperTransactionsAllocater preallocater = new ZooKeeperTransactionsAllocater(
				zkTestServer.getConnectString(),
				"fred",
				new TransactionNotification() {

					@Override
					public boolean transactionCreated(
							String clientID,
							String txID ) {
						synchronized (createdTXIds) {
							precreatedTXIds.add(txID);
							return true;
						}
					}

				});
		preallocater.preallocateTransactionIDs(
				10,
				"wilma");
		assertEquals(
				10,
				precreatedTXIds.size());
		final String txId = preallocater.getTransaction();
		assertTrue(precreatedTXIds.contains(txId));
		preallocater.releaseTransaction(txId);

	}

	@After
	public void stopZookeeper()
			throws IOException,
			InterruptedException {
		allocater.close();
		zkTestServer.stop();
	}

}
