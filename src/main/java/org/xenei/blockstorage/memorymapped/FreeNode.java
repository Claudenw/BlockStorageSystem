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
package org.xenei.blockstorage.memorymapped;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;

import org.xenei.blockstorage.MemoryMappedStorage;

/**
 * A free node on the free list.  This tracks the information
 * concerning a single buffer full of free nodes on the list by
 * extending the BlockHeader to add next block and block count 
 * information to the header.
 */
public class FreeNode extends BlockHeader {
	
	private static final int NEXT_BLOCK_OFFSET = BlockHeader.HEADER_SIZE;
	private static final int COUNT_OFFSET = NEXT_BLOCK_OFFSET + Long.BYTES;;
	/* package private for testing */
	static final int DATA_OFFSET = COUNT_OFFSET + Integer.BYTES;
	/* package private for testing */
	static final int DATA_SIZE = MemoryMappedStorage.BLOCK_SIZE - DATA_OFFSET;

	/**
	 * Constructor.
	 * @param bb the byte buffer to build this on.
	 * @throws IOException
	 */
	public FreeNode(ByteBuffer bb) throws IOException
	{
		super( bb );
	}
	
	/**
	 * Get the data space as a long buffer.
	 * @return the long buffer.
	 */
	public final LongBuffer getFreeRecords() {
		return getBuffer().position(DATA_OFFSET).asLongBuffer();
	}

	/**
	 * return true if this node only contains the header info.
	 * @return true if the node is "data" empty.
	 */
	public final boolean isEmpty() {
		return count() <= 0;
	}
	
	/**
	 * Get the next block in the chain.
	 * @return the next block or 0 for none.
	 */
	protected final long nextBlock() {
		return getBuffer().getLong(NEXT_BLOCK_OFFSET);
	}

	/**
	 * Set the next block in the chain.
	 * @param offset the offset of the next block may be 0 for none.
	 */
	protected final void nextBlock(long offset) {
		getBuffer().putLong(NEXT_BLOCK_OFFSET, offset);
	}

	/**
	 * Set the total number of records in the buffer.
	 * @param count
	 */
	public final void count(int count) {
		getBuffer().putInt(COUNT_OFFSET, count);
	}
	
	/**
	 * Get the total number of records in the buffer.
	 * @return the total number of used bytes.
	 */
	public final int count() {
		return getBuffer().getInt(COUNT_OFFSET);
	}

	@Override
	public String toString() {
		return String.format( "FreeNode[ o:%s u:%s l:%s c:%s fc:%s]", 
				offset(), buffUsed(), getBuffer().limit(), getBuffer().capacity(), count());
	}

}