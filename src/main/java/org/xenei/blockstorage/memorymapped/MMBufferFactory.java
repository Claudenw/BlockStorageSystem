package org.xenei.blockstorage.memorymapped;

import org.xenei.blockstorage.MemoryMappedStorage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.xenei.spanbuffer.lazy.tree.node.BufferFactory;

public class MMBufferFactory implements BufferFactory {
	private final MMFreeList freeBuffer;
	private final FileChannel fileChannel;

	public MMBufferFactory(MMFreeList freeBuffer, FileChannel fileChannel) {
		this.freeBuffer = freeBuffer;
		this.fileChannel = fileChannel;
	}

	@Override
	public int bufferSize() {
		return BlockHeader.BLOCK_SPACE;
	}

	@Override
	public int headerSize() {
		return BlockHeader.HEADER_SIZE;
	}

	@Override
	public ByteBuffer createBuffer() throws IOException {
		Long pos = freeBuffer.getBlock();
		if (pos == null) {
			pos = fileChannel.size();
		}

		MappedByteBuffer mBuffer = fileChannel.map(MapMode.READ_WRITE, pos, MemoryMappedStorage.BLOCK_SIZE).position(0);
		BlockHeader header = new BlockHeader(mBuffer);
		header.clear();
		header.offset(pos);
		return mBuffer.position(BlockHeader.HEADER_SIZE);
	}

	public ByteBuffer readBuffer(MMPosition position) throws IOException {
		if (position.isNoData()) {
			return createBuffer();
		}
		return fileChannel.map(MapMode.READ_WRITE, position.offset(), MemoryMappedStorage.BLOCK_SIZE)
				.position(BlockHeader.HEADER_SIZE);
	}

	@Override
	public void free(ByteBuffer buffer) throws IOException {
		BlockHeader header = new BlockHeader(buffer);
		long offset = header.offset();
		header.clear();
		freeBuffer.add(offset);
	}
}
