package org.xenei.blockstorage.memorymapped;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.blockstorage.MemoryMappedStorage;
import org.xenei.blockstorage.Stats;
import org.xenei.spanbuffer.lazy.tree.node.BufferFactory;

/**
 * The Buffer Factory.
 *
 */
public class MMBufferFactory implements BufferFactory, MMFreeList.BufferFactory {
	private MMFreeList freeList;
	private final FileChannel fileChannel;
	private final static Logger LOG = LoggerFactory.getLogger(MMBufferFactory.class);

	/**
	 * Constructor.
	 * 
	 * @param fileChannel the file channel.
	 * @param freeList    the free list.
	 * @throws IOException on error
	 */
	public MMBufferFactory(FileChannel fileChannel) throws IOException {
		this.fileChannel = fileChannel;
		freeList = new MMFreeList(this);
	}

	/**
	 * Get the stats block storage stats from the buffer factory.
	 * 
	 * @return the Stats for this block storage.
	 */
	public Stats getStats() {
		return new StatsImpl();
	}

	/**
	 * Close the system
	 * 
	 * @throws IOException on error
	 */
	public void close() throws IOException {
		fileChannel.close();
		freeList = null;
	}

	/**
	 * Get the freelist used by this buffer implementation.
	 * 
	 * @return the freelist.
	 */
	public MMFreeList getFreeList() {
		if (this.freeList == null) {
			throw new IllegalStateException("FreeList not set");
		}
		return this.freeList;

	}

	@Override
	public int bufferSize() {
		return BlockHeader.BLOCK_SPACE - BlockHeader.HEADER_SIZE;
	}

	@Override
	public int headerSize() {
		return BlockHeader.HEADER_SIZE;
	}

	@Override
	public ByteBuffer createBuffer() throws IOException {
		Long pos = freeList.getBlock();
		if (pos == null) {
			pos = fileChannel.size();
		}

		MappedByteBuffer mBuffer = fileChannel.map(MapMode.READ_WRITE, pos, MemoryMappedStorage.BLOCK_SIZE).position(0);
		BlockHeader header = new BlockHeader(mBuffer);
		header.clear();
		header.offset(pos);
		LOG.debug("Creating buffer {}", header);
		return mBuffer.position(BlockHeader.HEADER_SIZE);
	}

	/**
	 * Read a buffer.
	 * 
	 * @param position the position to read the buffer from, if position = -1 create
	 *                 new buffer.
	 * @return the read buffer.
	 * @throws IOException on error.
	 */
	@Override
	public ByteBuffer readBuffer(MMPosition position) throws IOException {
		if (position.isNoData()) {
			return createBuffer();
		}

		ByteBuffer buffer = fileChannel.map(MapMode.READ_WRITE, position.offset(), MemoryMappedStorage.BLOCK_SIZE)
				.position(BlockHeader.HEADER_SIZE);
		BlockHeader header = new BlockHeader(buffer);
		header.verifySignature();
		return buffer;
	}

	@Override
	public void free(ByteBuffer buffer) throws IOException {
		/*
		 * We don't do anything as we do not free the buffers but let the tracking
		 * system handle it.
		 */
		BlockHeader header = new BlockHeader(buffer);
		header.verifySignature();
		long offset = header.offset();
		LOG.debug("Freeing {}", header);
		if (offset == 0) {
			throw new IOException("Can not delete block 0");
		}
		if (header.is(BlockHeader.FREE_FLAG)) {
			throw new IOException("Can not free an already free block");
		}
		header.clear();
		header.sign();
		header.setFlag(BlockHeader.FREE_FLAG);
		freeList.add(offset);
	}

	/**
	 * Stats implementation foe MemoryMappedStorage.
	 *
	 */
	public class StatsImpl implements Stats {

		@Override
		public long dataLength() {
			try {
				return fileChannel.size();
			} catch (IOException e) {
				return -1;
			}
		}

		@Override
		public long deletedBlocks() {
			return (freeList == null) ? -1 : freeList.blockCount();
		}

		@Override
		public long freeSpace() {
			return (freeList == null) ? -1 : freeList.freeSpace();
		}

		@Override
		public String toString() {
			return String.format("l:%s f:%s d:%s", dataLength(), freeSpace(), deletedBlocks());
		}
	}
}