package com.cb.deng

import org.apache.flink.api.java.utils.ParameterTool

import org.apache.flink.streaming.api.windowing.time.Time

//需要加上这一行隐式转换 否则在调用flatmap方法的时候会报错
import org.apache.flink.api.scala._

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


/**
  * Created by dengcunbin on 2018/12/18.
  */
object AnotherWordCount {

  def main(args: Array[String]) {

    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    // get input data
    val text =
    // read the text file from given input path
      if (params.has("input")) {
        env.readTextFile(params.get("input"))
      } else {
        println("Executing WordCount example with default inputs data set.")
        println("Use --input to specify file input.")
        // get default test text data
        env.fromElements("hello","world","hello")
      }


    val counts: DataStream[(String, Int)] = text
      // split up the lines in pairs (2-tuples) containing: (word,1)
      .flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      // group by the tuple field "0" and sum up tuple field "1"
      .keyBy(0)
      .sum(1)

    // emit result
    if (params.has("output")) {
      counts.writeAsText(params.get("output"))
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      counts.print()
    }

    // execute program
    env.execute("Streaming WordCount")
  }
}
