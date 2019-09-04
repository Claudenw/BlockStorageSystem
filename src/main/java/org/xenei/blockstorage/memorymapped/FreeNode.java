package org.xenei.blockstorage.memorymapped;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.blockstorage.MemoryMappedStorage;

/**
 * A free node on the free list.
 *
 */
public class FreeNode extends BlockHeader {
	
	private static final int NEXT_BLOCK_OFFSET = BlockHeader.HEADER_SIZE;
	private static final int COUNT_OFFSET = NEXT_BLOCK_OFFSET + Long.BYTES;;
	/* package private for testing */
	static final int DATA_OFFSET = COUNT_OFFSET + Integer.BYTES;
	/* package private for testing */
	static final int DATA_SIZE = MemoryMappedStorage.BLOCK_SIZE - DATA_OFFSET;
	private static final Logger LOG = LoggerFactory.getLogger( FreeNode.class );

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