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
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.blockstorage.MemoryMappedStorage;
import org.xenei.spanbuffer.Factory;

/**
 * A block header is the first set of data in a block and is used by the system
 * to track it.
 *
 * The header contains the offset for the block, the number of bytes used in the
 * block.
 * 
 * The block is used both in the FreeList and in writing data.
 * 
 */
public class BlockHeader {
	public static final byte FREE_FLAG = 0x1;
	private static final Logger LOG = LoggerFactory.getLogger(BlockHeader.class);
	private static int SIGNATURE_SIZE = 5;
	private static ByteBuffer SIGNATURE = ByteBuffer.wrap(new byte[] { 0x0, (byte) 0xFF, 'B', 'H', 0x77 });
	private static byte[] CLEAN_BUFFER;
	// sig, status flag, file offset, used bytes
	public static final int HEADER_SIZE = SIGNATURE_SIZE + 1 + Long.BYTES + Integer.SIZE;
	public static final int BLOCK_SPACE = MemoryMappedStorage.BLOCK_SIZE - HEADER_SIZE;
	private static final int STATUS_OFFSET = SIGNATURE_SIZE;
	private static final int OFFSET_OFFSET = STATUS_OFFSET + 1;
	private static final int USED_OFFSET = OFFSET_OFFSET + Long.BYTES;

	private final ByteBuffer buffer;

	static {
		CLEAN_BUFFER = new byte[MemoryMappedStorage.BLOCK_SIZE];
		Arrays.fill(CLEAN_BUFFER, (byte) 0);
	}

	/**
	 * Create a block header from the buffer and the specified offset. The offset
	 * should be the position from which the buffer was read.
	 * 
	 * @param offset the offset of the buffer in the file.
	 * @param buffer the buffer.
	 * @throws IOException
	 */
	public BlockHeader(ByteBuffer buffer) throws IOException {
		this.buffer = buffer;
	}

	/**
	 * Mark the header as free. This operation clears all the buffer data and then
	 * sets the FREE_FLAG and the offset. All other data is destroyed.
	 */
	public void free() {
		long pos = offset();
		clear();
		offset(pos);
		setFlag(FREE_FLAG);
	}

	/**
	 * Add the block signature to the block. Every block in the system should start
	 * with the signature.
	 */
	public void sign() {
		buffer.duplicate().position(0).put(SIGNATURE);
	}

	/**
	 * Verify that the signature is set.
	 * 
	 * @throws IOException if the signature is not valid.
	 */
	public void verifySignature() throws IOException {

		if (Factory.wrap(buffer).startsWith(Factory.wrap(SIGNATURE))) {
			LOG.debug("{} Signature verified", this);
		} else {
			throw new IOException(this.toString() + " failed verification");
		}

	}

	/**
	 * Sets the specified byte flag in the status byte.
	 * 
	 * @param flag the flag to set.
	 */
	public void setFlag(byte flag) {
		buffer.put(STATUS_OFFSET, (byte) (flag | buffer.get(STATUS_OFFSET)));
	}

	/**
	 * Verifies a flag is set.
	 * 
	 * @param flag the flage to check.
	 * @return true if the flag is set, false otherwise.
	 */
	public boolean is(byte flag) {
		return getFlag(flag) == flag;
	}

	/**
	 * get the value for the flag if it is set..
	 * 
	 * Masks the status byte with the flag bytes returning only the bits in the flag
	 * that are also on in the status.
	 * 
	 * @param flag the flag to check.
	 * @return the status bytes with the bits masked.
	 */
	public byte getFlag(byte flag) {
		return (byte) (flag & buffer.get(STATUS_OFFSET));
	}

	/**
	 * Set the offset for this block.
	 * 
	 * @param offset the offset of the block
	 */
	public void offset(long offset) {
		buffer.putLong(OFFSET_OFFSET, offset);
	}

	/**
	 * Get the offset for this block.
	 * 
	 * @return the offset for this block.
	 */
	public long offset() {
		return buffer.getLong(OFFSET_OFFSET);
	}

	/**
	 * Set the number of bytes used in the buffer. This may be less than the size of
	 * the buffer.
	 * 
	 * @param buffUsed the number of bytes used.
	 */
	public void buffUsed(int buffUsed) {
		buffer.putInt(USED_OFFSET, buffUsed);
	}

	/**
	 * Get the number of bytes used in the buffer. This may be less than the size of
	 * the buffer.
	 * 
	 * @return the number of bytes used.
	 */
	public int buffUsed() {
		return buffer.getInt(USED_OFFSET);
	}

	/**
	 * Get the buffer this header is part of.
	 * 
	 * @return the buffer this header is part of.
	 */
	public ByteBuffer getBuffer() {
		return buffer;
	}

	@Override
	public String toString() {
		return String.format("H[ o:%s u:%s l:%s c:%s]", offset(), buffUsed(), buffer.limit(), buffer.capacity());
	}

	/**
	 * Clear (fill with null) the entire buffer from the beginning. Adds the sign
	 * data back in after the buffer is cleared.
	 */
	public void clear() {
		buffer.position(0);
		doClear();
		sign();
	}

	/**
	 * Clear (fill with null) the buffer from the current position.
	 */
	private void doClear() {
		while (buffer.hasRemaining()) {
			int limit = Integer.min(buffer.remaining(), MemoryMappedStorage.BLOCK_SIZE);
			buffer.put(CLEAN_BUFFER, 0, limit);
		}
	}

	/**
	 * Clear (fill with null) all user data in buffer. This is all the data after
	 * the header.
	 */
	public void clearData() {
		buffer.position(HEADER_SIZE);
		doClear();
	}
}
