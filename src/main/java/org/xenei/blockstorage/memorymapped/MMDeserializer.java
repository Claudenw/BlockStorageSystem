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

import org.xenei.blockstorage.MemoryMappedStorage;
import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.lazy.tree.TreeLazyLoader;
import org.xenei.spanbuffer.lazy.tree.node.InnerNode;
import org.xenei.spanbuffer.lazy.tree.serde.TreeDeserializer;

public class MMDeserializer implements TreeDeserializer<MMPosition> {
	private FileChannel fileChannel;
	private MMFreeList freeBuffer;

	public MMDeserializer(FileChannel fileChannel, MMFreeList freeBuffer) {
		this.freeBuffer = freeBuffer;
	}

	@Override
	public ByteBuffer deserialize(MMPosition position) throws IOException {
		if (position.isNoData()) {
			return ByteBuffer.allocate(0);
		}

		MappedByteBuffer root = fileChannel.map(MapMode.READ_WRITE, position.offset(), MemoryMappedStorage.BLOCK_SIZE);
		BlockHeader header = new BlockHeader(root);
		return root.position(BlockHeader.HEADER_SIZE).limit(header.buffUsed());
	}

	@Override
	public List<TreeLazyLoader<MMPosition, TreeDeserializer<MMPosition>>> extractLoaders(SpanBuffer buffer)
			throws IOException {
		List<TreeLazyLoader<MMPosition, TreeDeserializer<MMPosition>>> result = new ArrayList<TreeLazyLoader<MMPosition, TreeDeserializer<MMPosition>>>();
		try (DataInputStream ois = new DataInputStream(buffer.getInputStream())) {
			while (true) {
				try {
					int idx = ois.readInt();
					TreeLazyLoader<MMPosition, TreeDeserializer<MMPosition>> tll = new TreeLazyLoader<MMPosition, TreeDeserializer<MMPosition>>(
							new MMPosition(idx), this);
					result.add(tll);
				} catch (EOFException e) {
					return result;
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void delete(MMPosition rootPosition) throws IOException {
		if (rootPosition.isNoData()) {
			return;
		}

		MappedByteBuffer root = fileChannel.map(MapMode.READ_WRITE, rootPosition.offset(),
				MemoryMappedStorage.BLOCK_SIZE);

		BlockHeader header = new BlockHeader(root);
		byte bType = root.position(BlockHeader.HEADER_SIZE).get();
		/*
		 * if root is outer node type we just delete it (see after if) otherwise we need
		 * to read buffer a series of long values and delete those before we delete the
		 * root node.
		 */
		if (bType != InnerNode.OUTER_NODE_FLAG) {
			LongBuffer lb = root.position(BlockHeader.HEADER_SIZE + 1).asLongBuffer();

			if (bType == InnerNode.LEAF_NODE_FLAG) {
				long pos;
				while (0 != (pos = lb.get())) {
					MappedByteBuffer leaf = fileChannel.map(MapMode.READ_WRITE, pos, MemoryMappedStorage.BLOCK_SIZE);
					BlockHeader leafHeader = new BlockHeader(leaf);
					leafHeader.clear();
					freeBuffer.add(pos);
				}
			} else {
				// outer nodes
				long pos;
				while (0 != (pos = lb.get())) {
					delete(new MMPosition(pos));
				}
			}
		}

		// delete the root now
		header.clear();
		freeBuffer.add(rootPosition.offset());
	}

}
