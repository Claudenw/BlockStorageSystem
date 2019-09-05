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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.blockstorage.MemoryMappedStorage;
import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.lazy.tree.TreeLazyLoader;
import org.xenei.spanbuffer.lazy.tree.node.BufferFactory;
import org.xenei.spanbuffer.lazy.tree.node.InnerNode;
import org.xenei.spanbuffer.lazy.tree.serde.AbstractSerde;
import org.xenei.spanbuffer.lazy.tree.serde.TreeDeserializer;
import org.xenei.spanbuffer.lazy.tree.serde.TreeSerializer;

/**
 * A Portmanteau of the a Buffer Factory, Serializer, and Deserializer that are
 * used for the implementation.
 *
 */
public class MMSerde extends AbstractSerde<MMPosition> {
	private static final Logger LOG = LoggerFactory.getLogger(MMSerde.class);
	private final MMBufferFactory factory;
	private final MMSerializer serializer;
	private final MMDeserializer deserializer;

	/**
	 * Constructor.
	 * 
	 * @param freeList    the free list.
	 * @param fileChannel the file channel.
	 */
	public MMSerde(MMFreeList freeList, FileChannel fileChannel) {
		this(new MMBufferFactory(fileChannel, freeList));
	}

	/**
	 * Constructor.
	 * 
	 * @param factory the factory
	 */
	private MMSerde(MMBufferFactory factory) {
		LOG.debug( "Creating MMSerde");
		this.factory = factory;
		this.serializer = new MMSerializer(factory, factory.freeList);
		this.deserializer = new MMDeserializer(factory, factory.freeList);
		verify();
		LOG.debug( "Created MMSerde");
	}

	/**
	 * Serialize in a specific position.
	 * 
	 * @param position the position to write to.
	 * @param buffer   the buffer to write.
	 * @return position that was written to.
	 * @throws IOException on error
	 */
	public MMPosition serialize(MMPosition position, ByteBuffer buffer) throws IOException {
		return serializer.serialize(position, buffer);
	}

	/**
	 * Delete the specified position.
	 * 
	 * @param rootPosition the position to delete.
	 * @throws IOException on error.
	 */
	public void delete(MMPosition rootPosition) throws IOException {
		deserializer.delete(rootPosition);
	}

	@Override
	public TreeDeserializer<MMPosition> getDeserializer() {
		return deserializer;
	}

	@Override
	public TreeSerializer<MMPosition> getSerializer() {
		return serializer;
	}

	@Override
	public BufferFactory getFactory() {
		return factory;
	}

	/**
	 * The Serializer
	 *
	 */
	static class MMSerializer implements TreeSerializer<MMPosition> {

		private MMBufferFactory bufferFactory;
		private MMFreeList freeList;
		private static final Logger LOG = LoggerFactory.getLogger(MMSerializer.class);

		/**
		 * Constructor.
		 * 
		 * @param factory  the factory to use.
		 * @param freeList the free list.
		 */
		public MMSerializer(MMBufferFactory factory, MMFreeList freeList) {
			this.bufferFactory = factory;
			this.freeList = freeList;
		}

		@Override
		public int getMaxBufferSize() {
			return Integer.MAX_VALUE;
		}

		@Override
		public MMPosition serialize(ByteBuffer buffer) throws IOException {
			/*
			 * ByteBuffer is a memory mapped byte buffer so the data is already writen to
			 * the underlying system. We just need to extract the MMPosition informtion.
			 */
			BlockHeader header = new BlockHeader(buffer);
			header.buffUsed(buffer.position());
			LOG.debug("Serializing {}", header);
			if (header.offset() == 0) {
				throw new IOException("Can not serialize to buffer 0");
			}
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
			LOG.debug("Serializing {} to {}", header, position);

			buffer.position(0);
			ByteBuffer other = bufferFactory.readBuffer(position);
			other.position(0);
			other.put( buffer );
			BlockHeader oHeader = new BlockHeader(other);
			other.position(BlockHeader.HEADER_SIZE);
			other.put(buffer);
			oHeader.offset( position.offset() );
			freeList.add(header.offset());
			LOG.debug("Serialized {} to {}", oHeader, position);
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

	/**
	 * The Deserializer.
	 */
	private static class MMDeserializer implements TreeDeserializer<MMPosition> {
		private final MMBufferFactory factory;
		private final MMFreeList freeList;
		private static final Logger LOG = LoggerFactory.getLogger(MMDeserializer.class);

		/**
		 * Constructor.
		 * 
		 * @param fileChannel the file channel to write on.
		 * @param freeList    The freelist to use.
		 */
		public MMDeserializer(MMBufferFactory factory, MMFreeList freeList) {
			this.factory = factory;
			this.freeList = freeList;
		}

		@Override
		public ByteBuffer deserialize(MMPosition position) throws IOException {
			if (position.isNoData()) {
				return ByteBuffer.allocate(0);
			}
			LOG.debug("Deserializing {}", position);
			ByteBuffer buffer = factory.readBuffer(position);
			BlockHeader header = new BlockHeader(buffer);
			LOG.debug("Deserialized {} from {}", header, position);
			if (header.offset() != position.offset()) {
				String msg = String.format("Block header %s and position %s do not match.", header, position);
				LOG.error(msg);
				throw new IllegalStateException(msg);
			}
			if (header.buffUsed() == 0) {
				String msg = String.format("Read block %s with zero length from position %s", header, position);
				LOG.error(msg);
				throw new IllegalStateException(msg);
			}
			return buffer.position(BlockHeader.HEADER_SIZE).limit(header.buffUsed());
		}

		@Override
		public List<TreeLazyLoader<MMPosition>> extractLoaders(SpanBuffer buffer) throws IOException {
			List<TreeLazyLoader<MMPosition>> result = new ArrayList<TreeLazyLoader<MMPosition>>();
			try (DataInputStream ois = new DataInputStream(buffer.getInputStream())) {
				while (true) {
					try {
						int idx = ois.readInt();
						TreeLazyLoader<MMPosition> tll = new TreeLazyLoader<MMPosition>(new MMPosition(idx), this);
						result.add(tll);
					} catch (EOFException e) {
						return result;
					}
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		/**
		 * Delete a specific position.
		 * 
		 * @param rootPosition the position to delete.
		 * @throws IOException on error.
		 */
		public void delete(MMPosition rootPosition) throws IOException {
			if (rootPosition.isNoData()) {
				return;
			}

			ByteBuffer root = factory.readBuffer(rootPosition);
			BlockHeader header = new BlockHeader(root);
			byte bType = root.position(BlockHeader.HEADER_SIZE).get();
			LOG.debug("Deleting {} of type {}", header, bType);
			/*
			 * if root is not and Outer Node, we need to read buffer as series of long
			 * values and delete those before we delete the root node. The nodes we read
			 * will be either INNER or LEAF nodes.
			 */

			if (bType != InnerNode.OUTER_NODE_FLAG) {
				LongBuffer lb = root.position(BlockHeader.HEADER_SIZE + 1).asLongBuffer();
				long pos;
				while (0 != (pos = lb.get())) {
					MMPosition position = new MMPosition(pos);
					if (bType == InnerNode.LEAF_NODE_FLAG) {
						ByteBuffer leaf = factory.readBuffer(position);
						BlockHeader leafHeader = new BlockHeader(leaf);
						leafHeader.clear();
						freeList.add(pos);
					} else {
						// INNER node so cascade down.
						delete(new MMPosition(pos));
					}
				}
			}

			// delete the root now
			header.clear();
			freeList.add(rootPosition.offset());
		}

		@Override
		public int headerSize() {
			return BlockHeader.HEADER_SIZE;
		}

	}

	/**
	 * The Buffer Factory.
	 *
	 */
	private static class MMBufferFactory implements BufferFactory {
		private final MMFreeList freeList;
		private final FileChannel fileChannel;
		private final static Logger LOG = LoggerFactory.getLogger(MMBufferFactory.class);

		/**
		 * Constructor.
		 * 
		 * @param fileChannel the file channel.
		 * @param freeList    the free list.
		 */
		public MMBufferFactory(FileChannel fileChannel, MMFreeList freeList) {
			this.freeList = freeList;
			this.fileChannel = fileChannel;
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

			MappedByteBuffer mBuffer = fileChannel.map(MapMode.READ_WRITE, pos, MemoryMappedStorage.BLOCK_SIZE)
					.position(0);
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
		public ByteBuffer readBuffer(MMPosition position) throws IOException {
			if (position.isNoData()) {
				return createBuffer();
			}
			return fileChannel.map(MapMode.READ_WRITE, position.offset(), MemoryMappedStorage.BLOCK_SIZE)
					.position(BlockHeader.HEADER_SIZE);
		}

		@Override
		public void free(ByteBuffer buffer) throws IOException {
			/*
			 * We don't do anything as we do not free the buffers but let the tracking
			 * system handle it.
			 */
			BlockHeader header = new BlockHeader(buffer);
			long offset = header.offset();
			LOG.debug("Freeing {}", header);
			if (offset == 0) {
				throw new IOException("Can not delete block 0");
			}
			header.clear();
			freeList.add(offset);
		}

	}
}
