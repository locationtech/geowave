/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.datastore.cassandra.operations;

import java.util.concurrent.Semaphore;

import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.datastore.cassandra.util.CassandraUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

public class BatchedWrite extends
		BatchHandler implements
		AutoCloseable
{
	private final static Logger LOGGER = LoggerFactory.getLogger(BatchedWrite.class);
	// TODO: default batch size is tiny at 50 KB, reading recommendations re:
	// micro-batch writing
	// (https://dzone.com/articles/efficient-cassandra-write), we should be able
	// to gain some efficiencies for bulk ingests with batches if done
	// correctly, while other recommendations contradict this article and
	// suggest don't use batching as a performance optimization
	private static final boolean ASYNC = true;
	private final int batchSize;
	private final PreparedStatement preparedInsert;
	private final static int MAX_CONCURRENT_WRITE = 100;
	// only allow so many outstanding async reads or writes, use this semaphore
	// to control it
	private final Semaphore writeSemaphore = new Semaphore(
			MAX_CONCURRENT_WRITE);

	public BatchedWrite(
			final Session session,
			final PreparedStatement preparedInsert,
			final int batchSize ) {
		super(
				session);
		this.preparedInsert = preparedInsert;
		this.batchSize = batchSize;
	}

	public void insert(
			final GeoWaveRow row ) {
		final BoundStatement[] statements = CassandraUtils.bindInsertion(
				preparedInsert,
				row);
		for (final BoundStatement statement : statements) {
			insertStatement(
					row,
					statement);
		}
	}

	private void insertStatement(
			final GeoWaveRow row,
			final BoundStatement statement ) {
		if (ASYNC) {
			if (batchSize > 1) {
				final BatchStatement currentBatch = addStatement(
						row,
						statement);
				synchronized (currentBatch) {
					if (currentBatch.size() >= batchSize) {
						writeBatch(currentBatch);
					}
				}
			}
			else {
				try {
					executeAsync(statement);
				}
				catch (InterruptedException e) {
					LOGGER.warn(
							"async write semaphore interrupted",
							e);
					writeSemaphore.release();
				}
			}
		}
		else {
			session.execute(statement);
		}
	}

	private void writeBatch(
			final BatchStatement batch ) {
		try {
			executeAsync(batch);

			batch.clear();
		}
		catch (InterruptedException e) {
			LOGGER.warn(
					"async batch write semaphore interrupted",
					e);
			writeSemaphore.release();
		}
	}

	private void executeAsync(
			Statement statement )
			throws InterruptedException {
		writeSemaphore.acquire();
		final ResultSetFuture future = session.executeAsync(statement);
		Futures.addCallback(
				future,
				new IngestCallback(
						writeSemaphore),
				CassandraOperations.WRITE_RESPONSE_THREADS);
	}

	@Override
	public void close()
			throws Exception {
		for (final BatchStatement batch : batches.values()) {
			synchronized (batch) {
				writeBatch(batch);
			}
		}

		// need to wait for all asynchronous batches to finish writing
		// before exiting close() method
		writeSemaphore.acquire(MAX_CONCURRENT_WRITE);
		writeSemaphore.release(MAX_CONCURRENT_WRITE);
	}

	// callback class
	protected static class IngestCallback implements
			FutureCallback<ResultSet>
	{

		private final Semaphore semaphore;

		public IngestCallback(
				Semaphore semaphore ) {
			this.semaphore = semaphore;
		}

		@Override
		public void onSuccess(
				final ResultSet result ) {
			semaphore.release();
			// placeholder: put any logging or on success logic here.
		}

		@Override
		public void onFailure(
				final Throwable t ) {
			semaphore.release();
			// go ahead and wrap in a runtime exception for this case, but you
			// can do logging or start counting errors.
			throw new RuntimeException(
					t);
		}
	}
}
