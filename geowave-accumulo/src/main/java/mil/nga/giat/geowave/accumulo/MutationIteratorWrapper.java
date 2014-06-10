package mil.nga.giat.geowave.accumulo;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.IngestCallback;

import org.apache.accumulo.core.data.Mutation;

/**
 * This class is used internally within the ingest process of GeoWave to convert
 * each entry into a set of mutations and iterate through them (maintaining a
 * queue of mutations internally in the case where a single entry converts to
 * mulitple mutations)
 * 
 * @param <EntryType>
 *            The type of the entry
 */
public class MutationIteratorWrapper<EntryType> implements
		Iterator<Mutation>
{
	public static interface EntryToMutationConverter<EntryType>
	{
		public List<Mutation> convert(
				EntryType entry );
	}

	final private Iterator<EntryType> entryIterator;
	final private EntryToMutationConverter<EntryType> converter;
	final private Queue<Mutation> mutationQueue = new LinkedList<Mutation>();
	private final IngestCallback<EntryType> ingestCallback;
	private List<Mutation> lastEntryMutations;
	private EntryType lastEntry;

	public MutationIteratorWrapper(
			final Iterator<EntryType> entryIterator,
			final EntryToMutationConverter<EntryType> converter,
			final IngestCallback<EntryType> ingestCallback ) {
		this.entryIterator = entryIterator;
		this.converter = converter;
		this.ingestCallback = ingestCallback;
	}

	@Override
	public boolean hasNext() {
		if (!mutationQueue.isEmpty()) {
			return true;
		}
		return entryIterator.hasNext();
	}

	@Override
	public Mutation next() {
		while (mutationQueue.isEmpty() && entryIterator.hasNext()) {
			// fill mutation queue with mutations from the next entry
			final EntryType entry = entryIterator.next();
			final List<Mutation> mutations = converter.convert(entry);

			lastEntryMutations = mutations;
			lastEntry = entry;
			mutationQueue.addAll(mutations);
		}
		final Mutation retVal = mutationQueue.poll();
		if (mutationQueue.isEmpty() && (ingestCallback != null)) {
			// if the mutation queue is empty, then all mutations for the last
			// entry have been ingested
			notifyEntryIngested();
		}
		return retVal;
	}

	private synchronized void notifyEntryIngested() {
		if ((lastEntry != null) && (lastEntryMutations != null)) {
			if (ingestCallback != null) {
				final List<ByteArrayId> rowIds = new ArrayList<ByteArrayId>();
				for (final Mutation m : lastEntryMutations) {
					rowIds.add(new ByteArrayId(
							m.getRow()));
				}
				ingestCallback.entryIngested(
						rowIds,
						lastEntry);
			}
			lastEntryMutations = null;
			lastEntry = null;
		}
	}

	@Override
	public void remove() {
		if (!mutationQueue.isEmpty()) {
			mutationQueue.poll();
		}
		entryIterator.remove();
	}
}
