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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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
public class FileIOBench {
	private static final String SYNC_FILE_NAME = "sync_tmp";
	private static final String ASYNC_FILE_NAME = "async_tmp";
	private static final int FILE_LEN = 5 * 1024 * 1024; // 5M
	private static final int BUFFER_SIZE = 32 * 1024; // 32K
	private FileChannel fileChannel;
	private AsynchronousFileChannel asyncFileChannel;

	private ByteBuffer byteBuffer;

	@Setup
	public void setUp() throws IOException {
		FileWriter writer = new FileWriter(SYNC_FILE_NAME);
		for (int i = 0; i < FILE_LEN; ++i) {
			// 'A' - 'z'
			writer.write('A' + ThreadLocalRandom.current().nextInt(58));
		}
		writer.flush();
		writer.close();

		writer = new FileWriter(ASYNC_FILE_NAME);
		for (int i = 0; i < FILE_LEN; ++i) {
			writer.write('A' + ThreadLocalRandom.current().nextInt(58));
		}
		writer.flush();
		writer.close();

		fileChannel = new RandomAccessFile(SYNC_FILE_NAME, "r").getChannel();
		asyncFileChannel = AsynchronousFileChannel.open(Paths.get(ASYNC_FILE_NAME), StandardOpenOption.READ);

		byteBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE + 5);
	}

	@TearDown
	public void tearDown() throws IOException {
		byteBuffer.clear();
		fileChannel.close();
		asyncFileChannel.close();

		new File(SYNC_FILE_NAME).delete();
		new File(ASYNC_FILE_NAME).delete();
	}

	@Benchmark
	public void test1SyncIO(Blackhole bh) throws IOException {
		fileChannel.position(0);
		byteBuffer.position(0);

		int left = BUFFER_SIZE;
		while (true) {
			// We know there exist enough contents in the file, and never return -1.
			left -= fileChannel.read(byteBuffer);
			if (left <= 0) {
				break;
			}
		}
		bh.consume(byteBuffer);
	}

	@Benchmark
	public void testAsyncIO(Blackhole bh) throws ExecutionException, InterruptedException, IOException {
		byteBuffer.position(0);
		Future<Integer> future = asyncFileChannel.read(byteBuffer, 0);
		int left = BUFFER_SIZE;
		int read;
		while (true) {
			read = future.get();
			// We know there exist enough contents in the file, and never return -1.
			left -= read;

			if (left <= 0) {
				break;
			}

			future = asyncFileChannel.read(byteBuffer, read);
		}

		bh.consume(byteBuffer);
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + FileIOBench.class.getSimpleName() + ".*")
				.build();

		new Runner(opt).run();
	}
}

