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

package org.example;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.TypeHint;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;


public class DataSet1FlatMap {

	public static void main(String[] args) throws Exception {
		final String currentPath = System.getProperty("user.dir");
		final String inputFilename = currentPath+"/src/main/resources/input.txt";
		final String outputFilename = currentPath+"/src/main/resources/output.txt";
		Files.deleteIfExists(Paths.get(outputFilename));

//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		final ParameterTool parameterTool = ParameterTool.fromArgs(args);

		env.getConfig().setGlobalJobParameters(parameterTool);

		DataSet<String> text = env.readTextFile(inputFilename);
//		DataSet<String> text = env.fromElements("sdf","sdf", "aaa", "avb", "aaa", "bbb", "ccc", "aba");

		DataSet<String> words = text.flatMap((String line, Collector<String> out) -> Arrays.stream(line.split(",")).forEach(w -> out.collect(w))).returns(TypeInformation.of(String.class));
		DataSet<String> filtered = words.filter(line -> line.contains("a"));

		DataSet<Tuple2<String,Integer>> tokenized = filtered.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
			System.out.println("Processing: " + value);
			for (String word : value.toLowerCase().split("\\W+")) {
				if (!word.isEmpty()) {
					out.collect(new Tuple2<>(word, 1));
				}
			}
		}).returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));

		DataSet<Tuple2<String,Integer>> counts = tokenized.groupBy(0).sum(1);

//		counts.writeAsText(outputFilename).setParallelism(1);
//		env.execute("Flink Streaming Java API Skeleton");

		counts.print();
	}
}
