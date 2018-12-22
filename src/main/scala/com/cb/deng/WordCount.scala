package com.cb.deng


import org.apache.flink.api.java.utils.ParameterTool

import org.apache.flink.streaming.api.windowing.time.Time

//需要加上这一行隐式转换 否则在调用flatmap方法的时候会报错
import org.apache.flink.api.scala._

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Created by dengcunbin on 2018/12/18.
  */
object WordCount {

  def main(args: Array[String]) {

    // 定义一个数据类型保存单词出现的次数
    case class WordWithCount(word: String, count: Long)

    // port 表示需要连接的端口
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception => {
        System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'")
        9000
      }
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("localhost", port, '\n')

    val windowCounts = text
      .flatMap { w => w.split("\\s") }
      .map { w => WordWithCount(w, 1) }
      .keyBy("word")
      .timeWindow(Time.seconds(5), Time.seconds(1))
      .sum("count")

    windowCounts.print().setParallelism(1)


    env.execute("Socket Window WordCount")
  }
}
