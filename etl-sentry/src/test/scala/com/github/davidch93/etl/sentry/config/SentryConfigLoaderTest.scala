package com.github.davidch93.etl.sentry.config

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SentryConfigLoaderTest extends AnyFunSuite with Matchers {

  test("Reading a config fromConfigPath should have a complete config") {
    val configPath = "src/test/resources/config/github-etl-sentry-staging.json"
    val config = SentryConfigLoader.fromConfigPath(configPath)

    config.groupName shouldEqual "bronze-daily"
    config.maxThreads shouldEqual 2
    config.configBucket shouldEqual "gs://github-config-staging"
    config.tableWhitelist should contain theSameElementsAs Array(
      "bronze_daily_mysql.github_staging_orders",
      "bronze_daily_postgresql.github_staging_users",
      "bronze_daily_mongodb.github_staging_transactions"
    )

    val deequRepositoryConfig = config.deequRepositoryConfig
    deequRepositoryConfig.getProjectId shouldEqual "github-staging"
    deequRepositoryConfig.getRegion shouldEqual "asia-southeast1"
    deequRepositoryConfig.getDatasetId shouldEqual "bronze_sentry"
    deequRepositoryConfig.getCreateDisposition shouldEqual "CREATE_IF_NEEDED"
    deequRepositoryConfig.getTemporaryGcsBucket shouldEqual "gs://dataproc-temp-staging"

    val sourceBigQueryConfig = config.sourceBigQueryConfig
    sourceBigQueryConfig.getProjectId shouldEqual "github-staging"
    sourceBigQueryConfig.getRegion shouldEqual "asia-southeast1"
  }

  test("Reading a config when the config doesn't exist should produce a RuntimeException") {
    val configPath = "src/test/resources/config/example.json"

    val ex = intercept[RuntimeException] {
      SentryConfigLoader.fromConfigPath(configPath)
    }
    ex.getMessage shouldEqual s"Failed to load configuration from `$configPath`!"
  }
}