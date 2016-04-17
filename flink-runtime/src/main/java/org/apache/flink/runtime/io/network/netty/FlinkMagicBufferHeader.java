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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

/**
 * Created by winston on 13/04/2016.
 */
public class FlinkMagicBufferHeader {

	private final InputChannelID receiverId;
	private final int sequenceNumber;
	private final boolean isBuffer;
	private final int size;

	public FlinkMagicBufferHeader(InputChannelID receiverId, int sequenceNumber, boolean isBuffer, int size) {
		this.receiverId = receiverId;
		this.sequenceNumber = sequenceNumber;
		this.isBuffer = isBuffer;
		this.size = size;
	}

	public InputChannelID getReceiverId() {
		return receiverId;
	}

	public int getSequenceNumber() {
		return sequenceNumber;
	}

	public boolean isBuffer() {
		return isBuffer;
	}

	public int getSize() {
		return size;
	}

}
