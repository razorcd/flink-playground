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

import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.TypeHint;

import java.nio.file.Files;
import java.nio.file.Paths;


public class DataSet2Joins {

    public static void main(String[] args) throws Exception {
        final String currentPath = System.getProperty("user.dir");
        final String inputFilename = currentPath+"/src/main/resources/input2.txt";
        final String outputFilename = currentPath+"/src/main/resources/output2.txt";
        Files.deleteIfExists(Paths.get(outputFilename));

//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        env.getConfig().setGlobalJobParameters(parameterTool);

        DataSet<String> text = env.readTextFile(inputFilename);
        System.out.println("Source data from " + inputFilename);
        text.print();
        System.out.println();

        DataSet<Tuple2<Integer,String>> lines = text
                .map(line -> {
                    String[] split = line.split(",");
                    return new Tuple2<Integer, String>(Integer.parseInt(split[0]), split[1]);
                })
                .returns(new TypeHint<Tuple2<Integer, String>>() {});
        DataSet<Tuple2<Integer,String>> filteredText = lines.filter(line -> line.f1.contains("a"));
        filteredText.print();



        DataSet<Integer> numbers = env.fromElements(0,1,2,3,4,5,6,7,8);
        DataSet<Tuple2<Integer,String>> numberesTuple = numbers.map(number -> {
                    String processedNumber = "number:"+number;
                    return new Tuple2<Integer, String>(number, processedNumber);
                })
                .returns(new TypeHint<Tuple2<Integer, String>>() {});
//		DataSet<String> words = text.flatMap((String line, Collector<String> out) -> Arrays.stream(line.split(",")).forEach(w -> out.collect(w))).returns(TypeInformation.of(String.class));

        DataSet<Tuple3<Integer,String,String>> joinedData = filteredText
                .leftOuterJoin(numberesTuple, JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND)
                .where(0).equalTo(0)
                .with((Tuple2<Integer, String> left, Tuple2<Integer, String> right) -> {
                    String rightValue = (right != null) ? right.f1 : null;
                    return new Tuple3<>(left.f0, left.f1, rightValue);
                })
                .returns(new TypeHint<Tuple3<Integer,String,String>>() {});


        DataSet<Tuple3<String,String,Integer>> tokenized = joinedData
                .flatMap((Tuple3<Integer,String,String> values, Collector<Tuple3<String,String,Integer>> out) -> {
                    System.out.println("Processing: " + values);
                    out.collect(new Tuple3<String,String,Integer>(values.f1, values.f2, 1));
                })
                .returns(TypeInformation.of(new TypeHint<Tuple3<String,String,Integer>>() {}));
            DataSet<Tuple3<String,String,Integer>> counts = tokenized.groupBy(0).sum(2);

//		counts.writeAsText(outputFilename).setParallelism(1);
//		env.execute("Flink Streaming Java API Skeleton");

        counts.print();
    }
}
