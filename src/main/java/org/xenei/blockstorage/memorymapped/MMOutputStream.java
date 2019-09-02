package org.xenei.blockstorage.memorymapped;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.spanbuffer.lazy.tree.TreeOutputStream;
import org.xenei.spanbuffer.lazy.tree.node.TreeNode;

public class MMOutputStream extends TreeOutputStream {

	private MMPosition finalPosition;
	private static final Logger LOG = LoggerFactory.getLogger(MMOutputStream.class);

	public MMOutputStream(MMSerializer serializer, MMBufferFactory factory) throws IOException {
		this(0, serializer, factory);
	}

	public MMOutputStream(long finalPosition, MMSerializer serializer, MMBufferFactory factory) throws IOException {
		super(serializer, factory);
		this.finalPosition = new MMPosition(finalPosition);
	}

	@Override
	protected void writeRoot(final TreeNode rootNode) throws ExecutionException, InterruptedException, IOException {

		if (LOG.isDebugEnabled()) {
			LOG.debug("Obtained data for the Root Node, writing it");
		}
		if (finalPosition.isNoData()) {
			position = serializer.serialize(rootNode.getData());
		} else {
			position = ((MMSerializer) serializer).serialize(finalPosition, rootNode.getData());
		}
		rootNode.clearData();
	}

}