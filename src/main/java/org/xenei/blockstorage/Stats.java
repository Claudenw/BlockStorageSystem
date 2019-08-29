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
package org.xenei.blockstorage;

/**
 * Statistics for the storage system.
 *
 */
public interface Stats {

	/**
	 * The number of bytes occupied by all the data + free space.
	 * @return the total length of the storage.
	 */
	long dataLength();

	/**
	 * Number of deleted blocks.
	 * @return the number of deleted blocks.
	 */
	long deletedBlocks();

	/**
	 * The number of bytes accounted for in the deleted blocks.
	 * @return the number of free bytes.
	 */
	long freeSpace();

}