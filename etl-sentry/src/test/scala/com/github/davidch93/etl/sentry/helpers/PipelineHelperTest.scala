package com.github.davidch93.etl.sentry.helpers

import com.amazon.deequ.checks.{Check, CheckLevel}
import com.github.davidch93.etl.core.schema.{Field, SchemaLoader}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class PipelineHelperTest extends AnyFunSuite with Matchers {

  test("Executing jobs should execute all jobs and return true if all succeed") {
    val pipelineHelper = PipelineHelper(2)
    val tableWhitelist = Array("table1", "table2")
    val doJob: String => Boolean = _ => true

    val result = pipelineHelper.executeJobs(tableWhitelist, doJob)

    result shouldBe true
  }

  test("Executing jobs should return false if any job fails") {
    val pipelineHelper = PipelineHelper(2)
    val tableWhitelist = Array("table1", "table2", "table3")
    val doJob: String => Boolean = tableName => tableName != "table2"

    val result = pipelineHelper.executeJobs(tableWhitelist, doJob)

    result shouldBe false
  }

  test("Generating Deequ Checks should handle empty fields") {
    val pipelineHelper = PipelineHelper(2)
    val fields = List.empty[Field]

    val checks = pipelineHelper.generateChecks(fields)

    checks shouldBe empty
  }

  test("Generating Deequ Checks for MySQL schema should generate checks for field rules") {
    val pipelineHelper = PipelineHelper(2)

    val schemaFilePath = "src/test/resources/schema/mysql/github_staging_orders/schema.json"
    val table = SchemaLoader.loadTableSchema(schemaFilePath)

    val checks = pipelineHelper.generateChecks(table.getSchema.getFields.asScala.toList)

    checks should have size 5
    checks.head.toString shouldBe Check(CheckLevel.Error, "IS_PRIMARY_KEY")
      .isPrimaryKey("id", hint = Some("The value of this column must be unique and not NULL!")).toString
    checks(1).toString shouldBe Check(CheckLevel.Error, "IS_NON_NEGATIVE")
      .isNonNegative("amount", hint = Some("The value of this column must be positive!")).toString
    checks(2).toString shouldBe Check(CheckLevel.Error, "HAS_COMPLETENESS")
      .hasCompleteness("amount", _ >= 0.85, hint = Some("NULL values of this column should be below 0.15!")).toString
    checks(3).toString shouldBe Check(CheckLevel.Error, "IS_CONTAINED_IN")
      .isContainedIn("status", Array("PAID", "CANCELLED", "REMITTED"), hint = Some("The value of this column must be positive!")).toString
    checks(4).toString shouldBe Check(CheckLevel.Error, "IS_COMPLETE")
      .isComplete("created_at", hint = Some("The value of this column must not be NULL!")).toString
  }

  test("Generating Deequ Checks for PostgreSQL should generate checks for field rules") {
    val pipelineHelper = PipelineHelper(2)

    val schemaFilePath = "src/test/resources/schema/postgresql/github_staging_users/schema.json"
    val table = SchemaLoader.loadTableSchema(schemaFilePath)

    val checks = pipelineHelper.generateChecks(table.getSchema.getFields.asScala.toList)

    checks should have size 2
    checks.head.toString shouldBe Check(CheckLevel.Error, "IS_PRIMARY_KEY")
      .isPrimaryKey("id", hint = Some("The value of this column must be unique and not NULL!")).toString
    checks(1).toString shouldBe Check(CheckLevel.Error, "IS_UNIQUE")
      .isUnique("email", hint = Some("The value of this column must be unique!")).toString
  }

  test("Generating Deequ Checks without any rules defined should return an empty rules") {
    val pipelineHelper = PipelineHelper(2)

    val schemaFilePath = "src/test/resources/schema/mongodb/github_staging_transactions/schema.json"
    val table = SchemaLoader.loadTableSchema(schemaFilePath)

    val checks = pipelineHelper.generateChecks(table.getSchema.getFields.asScala.toList)

    checks should have size 0
  }
}
