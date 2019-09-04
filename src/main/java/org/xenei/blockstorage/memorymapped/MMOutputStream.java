package org.xenei.blockstorage.memorymapped;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.blockstorage.memorymapped.MMSerde.MMSerializer;
import org.xenei.spanbuffer.lazy.tree.TreeOutputStream;
import org.xenei.spanbuffer.lazy.tree.node.TreeNode;

public class MMOutputStream extends TreeOutputStream {

	/* the final position to write the root node to */
	private MMPosition finalPosition;
	private static final Logger LOG = LoggerFactory.getLogger(MMOutputStream.class);

	public MMOutputStream(MMSerde serde) throws IOException {
		this(MMPosition.NO_DATA.offset(), serde);
	}

	public MMOutputStream(long finalPosition, MMSerde serde) throws IOException {
		super(serde);
		this.finalPosition = new MMPosition(finalPosition);
	}

	@Override
	protected void writeRoot(final TreeNode rootNode) throws IOException {

		if (LOG.isDebugEnabled()) {
			LOG.debug("Writing root node to {}", finalPosition);
		}
		if (finalPosition.isNoData()) {
			position = serializer.serialize(rootNode.getData());
		} else {
			position = ((MMSerializer) serializer).serialize(finalPosition, rootNode.getData());
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("Root node written to {}", position);
		}
	}

}