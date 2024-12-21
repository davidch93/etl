package com.github.davidch93.etl.batch.config

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BatchConfigLoaderTest extends AnyFunSuite with Matchers {

  test("Reading a config fromConfigPath should have a complete config") {
    val configPath = "src/test/resources/config/github-etl-batch-staging.json"
    val config = BatchConfigLoader.fromConfigPath(configPath)

    config.groupName shouldEqual "bronze-daily"
    config.maxThreads shouldEqual 2
    config.dataLakeBucket shouldEqual "gs://github-data-lake-staging"
    config.dataPoolBucket shouldEqual "gs://github-data-pool-staging"
    config.configBucket shouldEqual "gs://github-config-staging"
    config.tableWhitelist should contain theSameElementsAs Array(
      "mysqlstaging.github_staging.orders",
      "postgresqlstaging.github_staging.users",
      "mongodbstaging.github_staging.transactions"
    )

    val kafkaConfig = config.kafkaConfig
    kafkaConfig.getBootstrapServers shouldEqual "localhost:9092"

    val bigQueryConfig = config.bigQueryConfig
    bigQueryConfig.getProjectId shouldEqual "github-staging"
    bigQueryConfig.getRegion shouldEqual "asia-southeast1"
    bigQueryConfig.getDatasetId shouldEqual "bronze"
    bigQueryConfig.getCreateDisposition shouldEqual "CREATE_IF_NEEDED"
    bigQueryConfig.getTemporaryGcsBucket shouldEqual "gs://dataproc-temp-staging"
  }

  test("Reading a config when the config doesn't exist should produce a RuntimeException") {
    val configPath = "src/test/resources/config/example.json"

    val ex = intercept[RuntimeException] {
      BatchConfigLoader.fromConfigPath(configPath)
    }
    ex.getMessage shouldEqual s"Failed to load configuration from `$configPath`!"
  }
}
