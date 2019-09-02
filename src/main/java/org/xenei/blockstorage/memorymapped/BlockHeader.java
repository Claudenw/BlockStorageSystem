package org.xenei.blockstorage.memorymapped;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.xenei.blockstorage.MemoryMappedStorage;

/**
 * A block header. Each block has a header as the first set of data.
 *
 */
public class BlockHeader {
	private static byte[] CLEAN_BUFFER;
	public static final int HEADER_SIZE = 2 * Long.BYTES + Integer.BYTES;
	public static final int BLOCK_SPACE = MemoryMappedStorage.BLOCK_SIZE - HEADER_SIZE;
	private static final int OFFSET_OFFSET = 0;
	private static final int USED_OFFSET = OFFSET_OFFSET + Long.BYTES;
	// private static final int NEXT_OFFSET = USED_OFFSET + Integer.BYTES;

	private final ByteBuffer buffer;

	static {
		CLEAN_BUFFER = new byte[MemoryMappedStorage.BLOCK_SIZE];
		Arrays.fill(CLEAN_BUFFER, (byte) 0);
	}
//	/** 
//	 * Create a block at the end of the file channel.
//	 * @param fileChannel the file channel to work with.
//	 * @throws IOException on error.
//	 */
//	public BlockHeader(FileChannel fileChannel) throws IOException {
//		long offset = fileChannel.size();
//		buffer= fileChannel.map(FileChannel.MapMode.READ_WRITE, offset, BLOCK_SIZE);
//		buffer.position(0);
//		buffer.putLong(MemoryMappedStorage.BLOCK_SIZE);
//		blockInfo = LongSpan.fromLength(offset, MemoryMappedStorage.BLOCK_SIZE);
//		buffUsed(BLOCK_SPACE);
//		nextBlock( 0 );
//		clear();
//	}
//
//	/**
//	 * Read the block header from the specified location.
//	 * @param span the Span for the block position and length.
//	 * @throws IOException on error.
//	 */
//	public BlockHeader(LongSpan span) throws IOException {
//		this(span.getOffset(), fileChannel.map(FileChannel.MapMode.READ_WRITE, span.getOffset(), span.getLength()));
//	}
//
//	/**
//	 * Read a block header from the specified position in the channel.
//	 * @param offset the offset to read from.
//	 * @throws IOException on error.
//	 */
//	public BlockHeader(long offset) throws IOException {
//		this(offset, fileChannel.map(FileChannel.MapMode.READ_WRITE, offset, BLOCK_SIZE));
//	}

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

	public void offset(long offset) {
		buffer.putLong(OFFSET_OFFSET, offset);
	}

	public long offset() {
		return buffer.getLong(OFFSET_OFFSET);
	}

//	/**
//	 * Set the next block
//	 * @param nextBlock the position of the next block.
//	 */
//	public void nextBlock(long nextBlock) {
//		buffer.putLong(NEXT_OFFSET, nextBlock);
//	}
//	
//	/**
//	 * Get the next block,
//	 * @return the next block or zero (0) if not set.
//	 */
//	public long nextBlock()
//	{
//		return buffer.getLong(NEXT_OFFSET);
//	}

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

	public ByteBuffer getBuffer() {
		return buffer;
	}

//	/**
//	 * Get the span buffer that contains the data for this block. 
//	 * @return the span buffer containing the data for this block.
//	 * @throws IOException
//	 */
//	public SpanBuffer getSpanBuffer() throws IOException {
//		List<SpanBuffer> lst = new ArrayList<SpanBuffer>();
//		SpanBuffer sb = Factory.wrap(buffer.position(0).duplicate()).cut(HEADER_SIZE);
//		if (sb.getLength() > buffUsed()) {
//			sb = sb.head(buffUsed());
//		}
//		lst.add(sb);
//		if (sb.getLength() < buffUsed()) {
//			long len = buffUsed() - sb.getLength();
//			lst.add(Factory.wrap(fileChannel.map(FileChannel.MapMode.READ_WRITE, sb.getEnd() + 1, len)));
//		}
//
//		if (nextBlock() != 0) {
//			BlockHeader nxtHeader = new BlockHeader(nextBlock());
//			lst.add(nxtHeader.getSpanBuffer());
//		}
//		return Factory.merge(lst.iterator());
//	}
//	
//	/**
//	 * copies data from a walker into the file stream.
//	 * @param walker the walker to read from.
//	 * @param len the maximum number of bytes to write.
//	 * @param buff the buffer to write with.
//	 * @return the number of bytes writen.
//	 * @throws IOException
//	 */
//	private void fillBlock( Walker walker, long len, byte[] buff) throws IOException
//	{
//		long bytesCopied = 0;
//		int limit = (int) Long.min(len, buff.length );
//		int bytesRead = 0;
//		while (walker.hasCurrent() && limit>0)
//		{
//			if (limit > buff.length)
//			{
//				bytesRead = walker.read(buff);
//			} else {
//				bytesRead = walker.read(buff, 0, limit);
//			}
//			buffer.put( buff, 0, bytesRead );
//			bytesCopied += bytesRead;				
//		}
//		if (bytesCopied < len)
//		{
//			Walker fillWalker = new FillBuffer( len-bytesCopied ).getWalker();
//			fillBlock( fillWalker, len-bytesCopied, buff );
//		}
//
//	}
//
//
//	private void write(Walker walker, byte[] buff) throws IOException {
//		buffer.position(HEADER_SIZE);
//		fillBlock( walker, buffer.remaining(), buff );
//	}	
//		while (walker.hasCurrent())
//		{
//			BlockHeader nextHeader = null;
//			if ( nextBlock() > 0)
//			{
//				
//			}
//			int bytesRead = walker.read(buff);
//			buffer.put( buff, 0, bytesRead );
//			limit -= bytesRead;
//		}
//		if (!walker.hasCurrent())
//		{
//			
//		}
//		buffer.put(src)
//		BufferOutputStream bos = new BufferOutputStream(buffer);
//		buffUsed(IOUtils.copyLarge(in, bos, 0, limit));
//		if (in.available() > 0) {
//			if (buffer.capacity() < blockInfo.getLength()) {
//				long len = buffUsed() - buffer.capacity();
//				long pos = blockInfo.getOffset() + buffer.capacity();
//				bos = new BufferOutputStream(fileChannel.map(FileChannel.MapMode.READ_WRITE, pos, len));
//				IOUtils.copyLarge(in, bos, 0, len);
//			}
//		}
//		if (in.available() > 0) {
//			BlockHeader nextHeader = null;
//			freeInfo.skipFlush = true;
//			if (nextBlock() == 0) {
//				if (freeInfo.useFreeList && !freeBuffer.isEmpty()) {
//					synchronized (freeBuffer) {
//						freeInfo.needsFlush = true;
//						nextHeader = new BlockHeader(freeBuffer.getBlock());
//					}
//				} else {
//					nextHeader = new BlockHeader(fileChannel);
//				}
//
//			}
//			nextBlock(nextHeader.blockInfo.getOffset());
//			nextHeader.write(in, freeInfo);
//		} else {
//			if (nextBlock() != 0) {
//				// remove extra blocks.
//				if (freeInfo.useFreeList) {
//					delete(nextBlock());
//					nextBlock(0);
//				} else {
//					long theBlock = nextBlock();
//					while (theBlock != 0) {
//						BlockHeader nextHeader = new BlockHeader(theBlock);
//						nextHeader.clear();
//						theBlock = nextHeader.nextBlock();
//					}
//
//				}
//			}
//		}
//		if (freeInfo.needsFlush && doFlush) {
//			writeFreeBlocks();
//		}
//	}

	public void clear() {
		buffer.position(0);
		buffer.put(CLEAN_BUFFER);
	}

	public void clearData() {
		buffer.position(HEADER_SIZE);
		buffer.put(CLEAN_BUFFER, 0, BLOCK_SPACE);
	}
}
