package mil.nga.giat.geowave.vector.transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import mil.nga.giat.geowave.vector.transaction.TransactionNotification;
import mil.nga.giat.geowave.vector.transaction.ZooKeeperTransactionsAllocater;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ZooKeeperTransactionAllocaterTest
{
	TestingServer zkTestServer;
	ZooKeeperTransactionsAllocater allocater;

	List<String> createdTXIds = new ArrayList<String>();
	List<Throwable> failures = new ArrayList<Throwable>();
	Set<String> activeTX = new HashSet<String>();
	volatile boolean shutdown = false;
	private Random random = new Random();

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
					public void transactionCreated(
							String clientID,
							String txID ) {
						synchronized (createdTXIds) {
							createdTXIds.add(txID);
						}

					}

				});
	}

	private int runTest() throws InterruptedException {
		Thread[] thr = new Thread[10];
		for (int i = 0; i < thr.length; i++) {
			thr[i] = new Thread(
					new TXRequester());
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

		int workDone = runTest();
		System.out.println("Total created transactionIDS " + createdTXIds.size());
		assertTrue(createdTXIds.size() <= workDone);

	}

	
	// not a good test in the sense that the data is not flushed to the server.
	// works...with discrepencies
	//@Test
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
								synchronized (activeTX) {
									activeTX.wait();
								}
								System.out.println("restart");
								zkTestServer.stop();
								File oldDir = zkTestServer.getTempDirectory();
								zkTestServer.close();
								zkTestServer = new TestingServer(
										12181,
										oldDir);

							}
							catch (Exception e) {
								ok = false;
								e.printStackTrace();
							}
						}
					}
				});
		thr.start();
		runTest();
		thr.join();
	}

	private class TXRequester implements
			Runnable
	{
		int s = 0;

		public void run() {
			while (s < 10 && !shutdown) {
				s++;
				try {
					Thread.sleep(100);
				}
				catch (InterruptedException e) {}
				try {
					String txID = allocater.getTransaction();
					synchronized (activeTX) {
						assert (!activeTX.contains(txID)); // throws assertion
															// error
						activeTX.add(txID);
						activeTX.notify();
					}
					try {
						Thread.sleep(200 + (random.nextInt()%200));
					}
					catch (InterruptedException e) {}
					allocater.releaseTransaction(txID);
					synchronized (activeTX) {
						activeTX.remove(txID);
					}
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

	@After
	public void stopZookeeper()
			throws IOException,
			InterruptedException {
		allocater.close();
		zkTestServer.stop();
	}

}
