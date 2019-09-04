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
/**
 * Classes used by memory mapped storage to read read big blocks of data.
 * 
 * There are two basic memory mapped structures:
 * <ol>
 * <li>The free list which is a forward linked list of pages (buffers) containing longs 
 * that are the offsets for the free blocks on the file system.  The froward links
 * between pages of offsets are specified in the BlockHeader.nextBlock().</li>
 * 
 * <li>The data structures which are lazy loaded SpanBuffer trees.</li>
 * </ol>
 */
package org.xenei.blockstorage.memorymapped;