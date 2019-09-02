package org.xenei.blockstorage.memorymapped;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.xenei.spanbuffer.lazy.tree.serde.TreeSerializer;

public class MMSerializer implements TreeSerializer<MMPosition> {

	private MMBufferFactory bufferFactory;
	private MMFreeList freeBuffer;

	public MMSerializer(MMBufferFactory factory, MMFreeList freeBuffer) {
		this.bufferFactory = factory;
		this.freeBuffer = freeBuffer;
	}

	@Override
	public int getMaxBufferSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public MMPosition serialize(ByteBuffer buffer) throws IOException {
		/*
		 * ByteBuffer is a memory mapped byte buffer so the data is already writen to
		 * the underlying system. We just need to extract the MMPosition informtion.
		 */
		BlockHeader header = new BlockHeader(buffer);
		return new MMPosition(header.offset());
	}

	/**
	 * Write the root to the specified position.
	 * 
	 * @param position the position to write to.
	 * @param buffer   the buffer to write to the position.
	 * @return the position argument.
	 * @throws IOException
	 */
	public MMPosition serialize(MMPosition position, ByteBuffer buffer) throws IOException {
		/*
		 * the byte buffer is memory mapped so it is already written to the underlying
		 * system. So we have to copy the data and release the buffer.
		 */
		BlockHeader header = new BlockHeader(buffer);
		if (header.offset() == position.offset()) {
			return serialize(buffer);
		}
		buffer.position(BlockHeader.HEADER_SIZE);
		ByteBuffer other = bufferFactory.readBuffer(position);
		BlockHeader oHeader = new BlockHeader(other);
		other.position(BlockHeader.HEADER_SIZE);
		other.put(buffer);
		oHeader.buffUsed(header.buffUsed());
		freeBuffer.add(header.offset());
		return position;
	}

	@Override
	public ByteBuffer serialize(MMPosition position) throws IOException {
		return ByteBuffer.allocate(Long.BYTES).putLong(position.offset()).flip();
	}

	@Override
	public MMPosition getNoDataPosition() {
		return MMPosition.NO_DATA;
	}

	@Override
	public int getPositionSize() {
		return Long.BYTES;
	}

}