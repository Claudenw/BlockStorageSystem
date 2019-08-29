/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xenei.blockstorage;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import org.xenei.spanbuffer.SpanBuffer;

/**
 * The BlockStorage interface.
 * 
 * When writing to the block storage the storage system will attempt to reuse 
 * deleted data blocks and then extend the file.  The system returns a block
 * number that is then used to access the data later. 
 *
 */
public interface Storage extends Closeable {

	/**
	 * The default block size used by storage engines.
	 */
	public final static int DEFAULT_BLOCK_SIZE = 2 * 1024;

	/**
	 * Get the stats.
	 * @return the stats for this storage.
	 */
	public Stats stats();

	/**
	 * Get the first record.  Since the storage system utilizes a free 
	 * list, and that free list may be in the first block, or there may be
	 * other overhead stored at the start of the file, this method returns the
	 * first usable datablock.  This block may be in use, may cause the file to be
	 * extended.
	 * @return the first application available block in the system.
	 * @throws IOException on error.
	 */
	public SpanBuffer getFirstRecord() throws IOException;

	/**
	 * Write the buffer into the first record.
	 * @param buffer the buffer to write.
	 * @throws IOException on error.
	 */
	public void setFirstRecord(SpanBuffer buffer) throws IOException;	

	/**
	 * Write the serializable object into the specified block.
	 * @param blockNumber the block number to write into
	 * @param s A serializable object
	 * @throws IOException on error.
	 */
	public void write(long blockNumber, Serializable s) throws IOException;

	/**
	 * Write the buffer into the specified block.
	 * @param blockNumber the block number to write into
	 * @param buff a SpanBuffer to write.
	 * @throws IOException on error.
	 */
	public void write(long blockNumber, SpanBuffer buff) throws IOException;

	/**
	 * Append the serializable object to the storage.  This call will attempt
	 * to use free space before it extends the file.
	 * 
	 * @param s the serializable object.
	 * @return the blockNumber for the stored object.
	 * @throws IOException on error.
	 */
	public long append(Serializable s) throws IOException;

	/**
	 * Append the buffer to the storage.  This call will attempt
	 * to use free space before it extends the file.
	 * 
	 * @param buff the SpanBuffer to write.
	 * @return the blockNumber for the stored object.
	 * @throws IOException on error.
	 */	
	public long append(SpanBuffer buff) throws IOException;

	/**
	 * Read a serialized object from the block.
	 * @param blockNumber the block to read.
	 * @return the deserialized object.
	 * @throws IOException on error.
	 * @throws ClassNotFoundException if the serialized class is not available on the classpath.
	 */
	public Serializable readObject(long blockNumber) throws IOException, ClassNotFoundException;

	/**
	 * Read a buffer from the block.
	 * @param blockNumber the block to read.
	 * @return A SpanBuffer containing the data.
	 * @throws IOException on error.
	 */
	public SpanBuffer read(long blockNumber) throws IOException;
	
	/**
	 * Delete a block and place it on the freelist.
	 * @param blockNumber the block number to delete.
	 * @throws IOException on error.
	 */
	public void delete(long blockNumber) throws IOException;

	/**
	 * Close the storage system.
	 * Attempting any operation on a closed system yields unpredictable results.
	 * @throws IOException on error.
	 */
	@Override
	public void close() throws IOException;
	
}
