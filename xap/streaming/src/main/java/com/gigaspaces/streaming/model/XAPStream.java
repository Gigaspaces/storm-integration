package com.gigaspaces.streaming.model;

import java.io.Serializable;

import com.gigaspaces.streaming.model.XAPStreamBatch.BatchInfo;

/**
 * This is the primary abstraction for streaming.  The
 * interface presumes little except a batching implementation.
 * 
 * @author DeWayne
 *
 * @param <T>
 */
public interface XAPStream<T> extends Serializable {
	/**
	 * @return the stream name
	 */
	String getName();
	/**
	 * Write a batch
	 * @param entry entries to be written 
	 */
	void writeBatch(T...entry);
	/**
	 * Read a batch of entries
	 * @param max the maximum number of entries to read
	 * @return the batch read
	 */
	XAPStreamBatch<T> readBatch(int max);
	/**
	 * Reads a batch that has already been read.
	 * @param info information to identify batch entries.  Contained in @see XAPStreamBatch.
	 * @return
	 */
	XAPStreamBatch<T> rereadBatch(BatchInfo info);
	/**
	 * Deletes a batch
	 * @param info information to identify batch entries.  Contained in @see XAPStreamBatch.
	 */
	void clearBatch(BatchInfo info);
	/**
	 * The stream size
	 * @return the number of entries in the stream
	 */
	int count();
	/**
	 * Clear all entries 
	 */
	void clear();
}
