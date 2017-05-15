/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.mapreduce;

import java.io.IOException;
import java.net.URI;

import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.Credentials;

/**
 * This class wraps an existing reduce context that will write hadoop writable
 * objects as a reduce context that writes the native object for ease of
 * implementing mapreduce jobs.
 * 
 * @param <KEYIN>
 *            The reduce context's input type
 * @param <VALUEIN>
 *            The reduce context's output type
 */
public class NativeReduceContext<KEYIN, VALUEIN> implements
		ReduceContext<KEYIN, VALUEIN, GeoWaveInputKey, Object>
{
	private final ReduceContext<KEYIN, VALUEIN, GeoWaveInputKey, ObjectWritable> writableContext;
	private final HadoopWritableSerializationTool serializationTool;

	public NativeReduceContext(
			final ReduceContext<KEYIN, VALUEIN, GeoWaveInputKey, ObjectWritable> writableContext,
			final AdapterStore adapterStore ) {
		this.writableContext = writableContext;
		this.serializationTool = new HadoopWritableSerializationTool(
				adapterStore);
	}

	public NativeReduceContext(
			final ReduceContext<KEYIN, VALUEIN, GeoWaveInputKey, ObjectWritable> writableContext,
			final HadoopWritableSerializationTool serializationTool ) {
		this.writableContext = writableContext;
		this.serializationTool = serializationTool;
	}

	// delegate everything, except the write method, for this transform the
	// object to a writable
	@Override
	public void write(
			final GeoWaveInputKey key,
			final Object value )
			throws IOException,
			InterruptedException {
		writableContext.write(
				key,
				serializationTool.toWritable(
						key.getAdapterId(),
						value));

	}

	@Override
	public TaskAttemptID getTaskAttemptID() {
		return writableContext.getTaskAttemptID();
	}

	@Override
	public void setStatus(
			final String msg ) {
		writableContext.setStatus(msg);
	}

	@Override
	public String getStatus() {
		return writableContext.getStatus();
	}

	@Override
	public boolean nextKey()
			throws IOException,
			InterruptedException {
		return writableContext.nextKey();
	}

	@Override
	public Configuration getConfiguration() {
		return writableContext.getConfiguration();
	}

	@Override
	public boolean nextKeyValue()
			throws IOException,
			InterruptedException {
		return writableContext.nextKeyValue();
	}

	@Override
	public float getProgress() {
		return writableContext.getProgress();
	}

	@Override
	public int hashCode() {
		return writableContext.hashCode();
	}

	@Override
	public Iterable<VALUEIN> getValues()
			throws IOException,
			InterruptedException {
		return writableContext.getValues();
	}

	@Override
	public Credentials getCredentials() {
		return writableContext.getCredentials();
	}

	@Override
	public Counter getCounter(
			final Enum<?> counterName ) {
		return writableContext.getCounter(counterName);
	}

	@Override
	public KEYIN getCurrentKey()
			throws IOException,
			InterruptedException {
		return writableContext.getCurrentKey();
	}

	@Override
	public JobID getJobID() {
		return writableContext.getJobID();
	}

	@Override
	public int getNumReduceTasks() {
		return writableContext.getNumReduceTasks();
	}

	@Override
	public Counter getCounter(
			final String groupName,
			final String counterName ) {
		return writableContext.getCounter(
				groupName,
				counterName);
	}

	@Override
	public VALUEIN getCurrentValue()
			throws IOException,
			InterruptedException {
		return writableContext.getCurrentValue();
	}

	@Override
	public Path getWorkingDirectory()
			throws IOException {
		return writableContext.getWorkingDirectory();
	}

	@Override
	public Class<?> getOutputKeyClass() {
		return writableContext.getOutputKeyClass();
	}

	@Override
	public OutputCommitter getOutputCommitter() {
		return writableContext.getOutputCommitter();
	}

	@Override
	public Class<?> getOutputValueClass() {
		return writableContext.getOutputValueClass();
	}

	@Override
	public Class<?> getMapOutputKeyClass() {
		return writableContext.getMapOutputKeyClass();
	}

	@Override
	public Class<?> getMapOutputValueClass() {
		return writableContext.getMapOutputValueClass();
	}

	@Override
	public String getJobName() {
		return writableContext.getJobName();
	}

	public boolean userClassesTakesPrecedence() {
		return writableContext.getConfiguration().getBoolean(
				MAPREDUCE_JOB_USER_CLASSPATH_FIRST,
				false);
	}

	@Override
	public boolean equals(
			final Object obj ) {
		return writableContext.equals(obj);
	}

	@Override
	public Class<? extends InputFormat<?, ?>> getInputFormatClass()
			throws ClassNotFoundException {
		return writableContext.getInputFormatClass();
	}

	@Override
	public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass()
			throws ClassNotFoundException {
		return writableContext.getMapperClass();
	}

	@Override
	public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass()
			throws ClassNotFoundException {
		return writableContext.getCombinerClass();
	}

	@Override
	public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass()
			throws ClassNotFoundException {
		return writableContext.getReducerClass();
	}

	@Override
	public Class<? extends OutputFormat<?, ?>> getOutputFormatClass()
			throws ClassNotFoundException {
		return writableContext.getOutputFormatClass();
	}

	@Override
	public Class<? extends Partitioner<?, ?>> getPartitionerClass()
			throws ClassNotFoundException {
		return writableContext.getPartitionerClass();
	}

	@Override
	public RawComparator<?> getSortComparator() {
		return writableContext.getSortComparator();
	}

	@Override
	public String getJar() {
		return writableContext.getJar();
	}

	@Override
	public RawComparator<?> getCombinerKeyGroupingComparator() {
		return writableContext.getCombinerKeyGroupingComparator();
	}

	@Override
	public RawComparator<?> getGroupingComparator() {
		return writableContext.getGroupingComparator();
	}

	@Override
	public boolean getJobSetupCleanupNeeded() {
		return writableContext.getJobSetupCleanupNeeded();
	}

	@Override
	public boolean getTaskCleanupNeeded() {
		return writableContext.getTaskCleanupNeeded();
	}

	@Override
	public boolean getProfileEnabled() {
		return writableContext.getProfileEnabled();
	}

	@Override
	public String getProfileParams() {
		return writableContext.getProfileParams();
	}

	@Override
	public IntegerRanges getProfileTaskRange(
			final boolean isMap ) {
		return writableContext.getProfileTaskRange(isMap);
	}

	@Override
	public String getUser() {
		return writableContext.getUser();
	}

	@Override
	public boolean getSymlink() {
		return writableContext.getSymlink();
	}

	@Override
	public Path[] getArchiveClassPaths() {
		return writableContext.getArchiveClassPaths();
	}

	@Override
	public URI[] getCacheArchives()
			throws IOException {
		return writableContext.getCacheArchives();
	}

	@Override
	public URI[] getCacheFiles()
			throws IOException {
		return writableContext.getCacheFiles();
	}

	@Override
	public Path[] getLocalCacheArchives()
			throws IOException {
		return writableContext.getLocalCacheArchives();
	}

	@Override
	public Path[] getLocalCacheFiles()
			throws IOException {
		return writableContext.getLocalCacheFiles();
	}

	@Override
	public Path[] getFileClassPaths() {
		return writableContext.getFileClassPaths();
	}

	@Override
	public String[] getArchiveTimestamps() {
		return writableContext.getArchiveTimestamps();
	}

	@Override
	public String[] getFileTimestamps() {
		return writableContext.getFileTimestamps();
	}

	@Override
	public int getMaxMapAttempts() {
		return writableContext.getMaxMapAttempts();
	}

	@Override
	public void progress() {
		writableContext.progress();
	}

	@Override
	public String toString() {
		return writableContext.toString();
	}

	@Override
	public int getMaxReduceAttempts() {
		return writableContext.getMaxReduceAttempts();
	}

}
