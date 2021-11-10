package demo

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, tableConversions}
import org.apache.flink.table.api.{$, AnyWithOperations, EnvironmentSettings, Slide, Table, lit}
import org.apache.flink.types.Row
import org.apache.flink.streaming.api.scala._

/**
 * Author:BYDylan
 * Date:2021/11/10
 * Description:
 */
object HotItemsWithSql {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    val dataStream: DataStream[UserBehavior] = inputStream
      .map(data => {
        val arr = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 定义表执行环境
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    // 基于DataStream创建Table
    val dataTable: Table = tableEnv.fromDataStream(dataStream, $("itemId"), $("behavior"), $("timestamp"))

    // 1. Table API进行开窗聚合统计
    val aggTable = dataTable
      .select($("itemId"), $("behavior"))
      .filter("behavior=='pv'")
      //      .filter($("behavior").isNotNull)
      .window(Slide.over(lit(1).hours()).every(lit(5).minutes()).on($("ts")).as("sw"))
      .groupBy($("itemId"), $("sw"))
      .select($("itemId"), $("sw").as("windowEnd"), $("itemId").count().as("cnt"))



    // 用SQL去实现TopN的选取
    tableEnv.createTemporaryView("aggtable", aggTable)
    val resultTable = tableEnv.sqlQuery(
      """
        |select *
        |from (
        |  select
        |    *,
        |    row_number()
        |      over (partition by windowEnd order by cnt desc)
        |      as row_num
        |    from aggtable )
        |where row_num <= 5
      """.stripMargin)

    // 纯SQL实现
    tableEnv.createTemporaryView("datatable", dataStream)
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select *
        |from (
        |  select
        |    *,
        |    row_number()
        |      over (partition by windowEnd order by cnt desc)
        |      as row_num
        |    from (
        |      select
        |        itemId,
        |        hop_end(ts, interval '5' minute, interval '1' hour) as windowEnd,
        |        count(itemId) as cnt
        |      from datatable
        |      where behavior = 'pv'
        |      group by
        |        itemId,
        |        hop(ts, interval '5' minute, interval '1' hour)
        |    )
        |)
        |where row_num <= 5
      """.stripMargin)

    resultTable.toRetractStream[Row].print()

    env.execute("hot items sql job")
  }
}
