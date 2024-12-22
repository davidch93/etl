package com.github.davidch93.etl.batch.helpers

import com.github.davidch93.etl.core.config.BigQueryConfig
import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.LinkedBlockingQueue

class PipelineHelperTest extends AnyFunSuite with Matchers {

  test("Executing jobs should execute all jobs and return true if all succeed") {
    val bigQueryConfig = createBigQueryConfig()
    val pipelineHelper = PipelineHelper("test", 2, bigQueryConfig)

    val actualRows = new LinkedBlockingQueue[Row]()
    val tableWhitelist = Array(
      "mysqlstaging.github_staging.orders",
      "postgresqlstaging.github_staging.users",
      "mongodbstaging.github_staging.transactions",
      "dynamodbstaging.github_staging.transactions"
    )

    val allJobsSucceeded = pipelineHelper.executeJobs(
      tableWhitelist,
      _ => true,
      row => actualRows.add(Row(row(0), row(4), row(5)))
    )

    val expectedRows = Array(
      Row("bronze_daily_mysql.github_staging_orders", true, ""),
      Row("bronze_daily_postgresql.github_staging_users", true, ""),
      Row("bronze_daily_mongodb.github_staging_transactions", true, ""),
      Row("bronze_daily_dynamodb.github_staging_transactions", true, "")
    )

    allJobsSucceeded shouldBe true
    actualRows.toArray should have size 4
    actualRows.toArray should contain theSameElementsAs expectedRows
  }

  test("Executing jobs should return false if any job fails") {
    val bigQueryConfig = createBigQueryConfig()
    val pipelineHelper = PipelineHelper("test", 2, bigQueryConfig)

    val actualRows = new LinkedBlockingQueue[Row]()
    val tableWhitelist = Array(
      "mysqlstaging.github_staging.orders",
      "postgresqlstaging.github_staging.users",
      "mongodbstaging.github_staging.transactions",
      "dynamodbstaging.github_staging.transactions"
    )

    val allJobsSucceeded = pipelineHelper.executeJobs(
      tableWhitelist,
      topic => throw new RuntimeException(s"Job failed for $topic!"),
      row => actualRows.add(Row(row(0), row(4), row.getAs[String](5).split("\n").head))
    )

    val expectedRows = Array(
      Row("bronze_daily_mysql.github_staging_orders", false, "java.lang.RuntimeException: Job failed for mysqlstaging.github_staging.orders!"),
      Row("bronze_daily_postgresql.github_staging_users", false, "java.lang.RuntimeException: Job failed for postgresqlstaging.github_staging.users!"),
      Row("bronze_daily_mongodb.github_staging_transactions", false, "java.lang.RuntimeException: Job failed for mongodbstaging.github_staging.transactions!"),
      Row("bronze_daily_dynamodb.github_staging_transactions", false, "java.lang.RuntimeException: Job failed for dynamodbstaging.github_staging.transactions!")
    )

    allJobsSucceeded shouldBe false
    actualRows.toArray should have size 4
    actualRows.toArray should contain theSameElementsAs expectedRows
  }

  test("Executing jobs should handle empty whitelist tables") {
    val bigQueryConfig = createBigQueryConfig()
    val pipelineHelper = PipelineHelper("test", 2, bigQueryConfig)

    val allJobsSucceeded = pipelineHelper.executeJobs(
      Array.empty[String],
      _ => true,
      _ => true
    )

    allJobsSucceeded shouldBe true
  }

  private def createBigQueryConfig(): BigQueryConfig = {
    val config = new BigQueryConfig()
    config.setProjectId("github-staging")
    config.setRegion("asia-southeast1")
    config.setDatasetId("bronze")
    config
  }
}
