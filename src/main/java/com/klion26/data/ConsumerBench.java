/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.klion26.data;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.Throughput;
import static org.openjdk.jmh.annotations.Scope.Thread;

@SuppressWarnings("MethodMayBeStatic")
@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(Throughput)
@Fork(value = 3, jvmArgsAppend = {
		"-Djava.rmi.server.hostname=127.0.0.1",
		"-Dcom.sun.management.jmxremote.authenticate=false",
		"-Dcom.sun.management.jmxremote.ssl=false",
		"-Dcom.sun.management.jmxremote.ssl"})
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class ConsumerBench {

	private ByteBuffer buffer;
	private ByteBuffer endOfPartitionEventEventBuffer;
	private ByteBuffer checkpointBarrierEventBuffer;
	private ByteBuffer cancelCheckpointMarkerEventBuffer;

	// 32K  align with FileIOBench#BUFFER_SIZE
	private static int BUFFER_SIZE = 32 * 1024;
	private static int pageSize = 32 * 1024; // 32K

	private static int HEADER_LENGTH = 9;
	private byte[] bytes = new byte[pageSize + 10];

	@Setup
	public void setUp() throws IOException {
		buffer = ByteBuffer.allocateDirect(BUFFER_SIZE + 20);
		// channel
		buffer.putInt(0);
		// length
		buffer.putInt(BUFFER_SIZE);
		// buffer
		buffer.put((byte) 0);

		for (int i = 0; i < BUFFER_SIZE; ++i) {
			buffer.put((byte) ThreadLocalRandom.current().nextInt(128));
		}

		// endOfPartitionEventEventBuffer
		endOfPartitionEventEventBuffer = ByteBuffer.allocateDirect(20);
		ByteBuffer byteBuffer = EventSerializer.toSerializedEvent(EndOfPartitionEvent.INSTANCE);
		// channel
		endOfPartitionEventEventBuffer.putInt(0);
		// length
		endOfPartitionEventEventBuffer.putInt(byteBuffer.remaining());
		// event
		endOfPartitionEventEventBuffer.put((byte) 1);
		endOfPartitionEventEventBuffer.put(byteBuffer);

		// cancelCheckpointMarkerEventBuffer
		cancelCheckpointMarkerEventBuffer = ByteBuffer.allocateDirect(30);
		byteBuffer = EventSerializer.toSerializedEvent(new CancelCheckpointMarker(123));
		cancelCheckpointMarkerEventBuffer.putInt(0);
		cancelCheckpointMarkerEventBuffer.putInt(byteBuffer.remaining());
		cancelCheckpointMarkerEventBuffer.put((byte) 1);
		cancelCheckpointMarkerEventBuffer.put(byteBuffer);

		// checkpoint barrier
		checkpointBarrierEventBuffer = ByteBuffer.allocateDirect(40);
		byteBuffer = EventSerializer.toSerializedEvent(new CheckpointBarrier(1, 1511233451, CheckpointOptions.forCheckpointWithDefaultLocation()));
		checkpointBarrierEventBuffer.putInt(0);
		checkpointBarrierEventBuffer.putInt(byteBuffer.remaining());
		checkpointBarrierEventBuffer.put((byte) 1);
		checkpointBarrierEventBuffer.put(byteBuffer);
	}

	@TearDown
	public void tearDown() {
		buffer.clear();
	}

	@Benchmark
	public void testBufferWithWrap(Blackhole bh) throws IOException {
		buffer.position(0);
		final int channel = buffer.getInt();
		final int length = buffer.getInt();
		final boolean isBuffer = buffer.get() == 0;

		// deserialize buffer
		if (length > pageSize) {
			throw new IOException(String.format("Spilled buffer (%d bytes) is larger than page size of (%d bytes)",
					length,
					pageSize));
		}

		MemorySegment seg = MemorySegmentFactory.wrap(bytes);
		int segPos = 0;
		int bytesRemaining = length;

		while (true) {
			int toCopy = Math.min(buffer.remaining(), bytesRemaining);
			if (toCopy > 0) {
				seg.put(segPos, buffer, toCopy);
				segPos += toCopy;
				bytesRemaining -= toCopy;
			}

			if (bytesRemaining == 0) {
				break;
			}
		}

		Buffer buf = new NetworkBuffer(seg, FreeingBufferRecycler.INSTANCE);
		buf.setSize(length);

		bh.consume(isBuffer);
		bh.consume(channel);
		bh.consume(buf);
	}

	@Benchmark
	public void testBuffer(Blackhole bh) throws IOException {
		buffer.position(0);
		final int channel = buffer.getInt();
		final int length = buffer.getInt();
		final boolean isBuffer = buffer.get() == 0;

		// deserialize buffer
		if (length > pageSize) {
			throw new IOException(String.format("Spilled buffer (%d bytes) is larger than page size of (%d bytes)",
					length,
					pageSize));
		}

		MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(pageSize);

		int segPos = 0;
		int bytesRemaining = length;

		while (true) {
			int toCopy = Math.min(buffer.remaining(), bytesRemaining);
			if (toCopy > 0) {
				seg.put(segPos, buffer, toCopy);
				segPos += toCopy;
				bytesRemaining -= toCopy;
			}

			if (bytesRemaining == 0) {
				break;
			}
		}

		Buffer buf = new NetworkBuffer(seg, FreeingBufferRecycler.INSTANCE);
		buf.setSize(length);

		bh.consume(isBuffer);
		bh.consume(channel);
		bh.consume(buf);
	}

	@Benchmark
	// same as END_OF_SUPERSTEP_EVENT
	public void testEndOfPartitionEvent(Blackhole bh) throws IOException {
		endOfPartitionEventEventBuffer.position(0);

		final int channel = endOfPartitionEventEventBuffer.getInt();
		final int length = endOfPartitionEventEventBuffer.getInt();
		final boolean isBuffer = endOfPartitionEventEventBuffer.get() == 0;

		if (length > endOfPartitionEventEventBuffer.capacity() - HEADER_LENGTH) {
			throw new IOException("Event is too large");
		}

		int oldLimit = endOfPartitionEventEventBuffer.limit();
		endOfPartitionEventEventBuffer.limit(endOfPartitionEventEventBuffer.position() + length);
		AbstractEvent evt = EventSerializer.fromSerializedEvent(endOfPartitionEventEventBuffer, getClass().getClassLoader());
		endOfPartitionEventEventBuffer.limit(oldLimit);

		bh.consume(channel);
		bh.consume(isBuffer);
		bh.consume(evt);
	}

	@Benchmark
	public void testCancelCheckpointMarkerEvent(Blackhole bh) throws IOException {
		cancelCheckpointMarkerEventBuffer.position(0);

		final int channel = cancelCheckpointMarkerEventBuffer.getInt();
		final int length = cancelCheckpointMarkerEventBuffer.getInt();
		final boolean isBuffer = cancelCheckpointMarkerEventBuffer.get() == 0;

		if (length > cancelCheckpointMarkerEventBuffer.capacity() - HEADER_LENGTH) {
			throw new IOException("Event is too large");
		}

		int oldLimit = cancelCheckpointMarkerEventBuffer.limit();
		cancelCheckpointMarkerEventBuffer.limit(cancelCheckpointMarkerEventBuffer.position() + length);
		AbstractEvent evt = EventSerializer.fromSerializedEvent(cancelCheckpointMarkerEventBuffer, getClass().getClassLoader());
		cancelCheckpointMarkerEventBuffer.limit(oldLimit);

		bh.consume(channel);
		bh.consume(isBuffer);
		bh.consume(evt);
	}

	@Benchmark
	public void testCheckpointBarrierEvent(Blackhole bh) throws IOException {
		checkpointBarrierEventBuffer.position(0);

		final int channel = checkpointBarrierEventBuffer.getInt();
		final int length = checkpointBarrierEventBuffer.getInt();
		final boolean isBuffer = checkpointBarrierEventBuffer.get() == 0;

		if (length > checkpointBarrierEventBuffer.capacity() - HEADER_LENGTH) {
			throw new IOException("Event is too large");
		}

		int oldLimit = checkpointBarrierEventBuffer.limit();
		checkpointBarrierEventBuffer.limit(checkpointBarrierEventBuffer.position() + length);
		AbstractEvent evt = EventSerializer.fromSerializedEvent(checkpointBarrierEventBuffer, getClass().getClassLoader());
		checkpointBarrierEventBuffer.limit(oldLimit);

		bh.consume(channel);
		bh.consume(isBuffer);
		bh.consume(evt);
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + ConsumerBench.class.getSimpleName() + ".*")
				.build();

		new Runner(opt).run();
	}
}

