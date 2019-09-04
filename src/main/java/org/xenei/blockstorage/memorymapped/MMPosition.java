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

import org.xenei.spanbuffer.lazy.tree.serde.Position;

/**
 * The definition of a Position in the system.
 *
 */
public class MMPosition implements Position {
	private final long offset;

	/**
	 * A position that indicates no data.
	 */
	public static final MMPosition NO_DATA = new MMPosition(-1);

	/**
	 * Constructor.
	 * 
	 * If the offset is specified as a negative position, the position
	 * is set to indicate no data.
	 * 
	 * @param offset the file offset for the position.
	 */
	public MMPosition(long offset) {
		if (offset < 0)
		{
			offset = -1;
		}
		this.offset = offset;
	}

	@Override
	public boolean isNoData() {
		return offset == -1;
	}

	/**
	 * Get the file offset for this position.
	 * @return the offset.
	 */
	public long offset() {
		return offset;
	}

	@Override
	public String toString() {
		return Long.toString(offset);
	}
}
