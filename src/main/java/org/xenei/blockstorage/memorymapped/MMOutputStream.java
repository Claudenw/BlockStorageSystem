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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.blockstorage.memorymapped.MMSerde.MMSerializer;
import org.xenei.spanbuffer.lazy.tree.TreeOutputStream;
import org.xenei.spanbuffer.lazy.tree.node.TreeNode;

/**
 * A TreeOutputStream implementation the memory mapped data writing. The
 * difference between this implementation and the standard implementation is
 * that this implementation the accepts a position in which to place the root
 * node of the tree.
 *
 */
public class MMOutputStream extends TreeOutputStream {

	/* the final position to write the root node to */
	private MMPosition finalPosition;
	private static final Logger LOG = LoggerFactory.getLogger(MMOutputStream.class);

	/**
	 * Constructor.
	 * 
	 * @param serde the MMSerde describing the storage.
	 * @throws IOException on error.
	 */
	public MMOutputStream(MMSerde serde) throws IOException {
		this(MMPosition.NO_DATA.offset(), serde);
	}

	/**
	 * Constructor that writes to a specific location.
	 * 
	 * @param finalPosition the position where the root node will be written.
	 * @param serde         the MMSerde describing the storage.
	 * @throws IOException on error
	 */
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
			position = serializer.serialize(rootNode.getRawBuffer());
		} else {
			position = ((MMSerializer) serializer).serialize(finalPosition, rootNode.getRawBuffer());
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("Root node written to {}", position);
		}
	}

}