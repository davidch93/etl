package com.github.davidch93.etl.core.utils;

import com.github.davidch93.etl.core.constants.Source;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class BigQueryTableResolverTest {

    @Test
    void testResolve_withMySqlBigQueryTable_shouldResolveParameters() {
        String bigQueryTable = "bronze_daily_mysql.github_staging_orders";
        BigQueryTableResolver resolver = BigQueryTableResolver.resolve(bigQueryTable);

        assertThat(resolver).isNotNull();
        assertThat(resolver.identifySource()).isEqualTo(Source.MYSQL);
        assertThat(resolver.getTableName()).isEqualTo("github_staging_orders");
    }

    @Test
    void testResolve_withPostgreSqlBigQueryTable_shouldResolveParameters() {
        String bigQueryTable = "bronze_daily_postgresql.github_staging_users";
        BigQueryTableResolver resolver = BigQueryTableResolver.resolve(bigQueryTable);

        assertThat(resolver).isNotNull();
        assertThat(resolver.identifySource()).isEqualTo(Source.POSTGRESQL);
        assertThat(resolver.getTableName()).isEqualTo("github_staging_users");
    }

    @Test
    void testResolve_withMongoDbBigQueryTable_shouldResolveParameters() {
        String bigQueryTable = "bronze_daily_mongodb.github_staging_transactions";
        BigQueryTableResolver resolver = BigQueryTableResolver.resolve(bigQueryTable);

        assertThat(resolver).isNotNull();
        assertThat(resolver.identifySource()).isEqualTo(Source.MONGODB);
        assertThat(resolver.getTableName()).isEqualTo("github_staging_transactions");
    }

    @Test
    void testResolve_withDynamoDbBigQueryTable_shouldResolveParameters() {
        String bigQueryTable = "bronze_daily_dynamodb.github_staging_transactions";
        BigQueryTableResolver resolver = BigQueryTableResolver.resolve(bigQueryTable);

        assertThat(resolver).isNotNull();
        assertThat(resolver.identifySource()).isEqualTo(Source.DYNAMODB);
        assertThat(resolver.getTableName()).isEqualTo("github_staging_transactions");
    }

    @Test
    void testResolve_withDataWarehouseBigQueryTable_shouldResolveParameters() {
        String bigQueryTable = "data_warehouse.fact_transactions";
        BigQueryTableResolver resolver = BigQueryTableResolver.resolve(bigQueryTable);

        assertThat(resolver).isNotNull();
        assertThat(resolver.identifySource()).isEqualTo(Source.DATA_WAREHOUSE);
        assertThat(resolver.getTableName()).isEqualTo("fact_transactions");
    }

    @Test
    void testResolve_withDataMartBigQueryTable_shouldResolveParameters() {
        String bigQueryTable = "data_mart.top_buyers";
        BigQueryTableResolver resolver = BigQueryTableResolver.resolve(bigQueryTable);

        assertThat(resolver).isNotNull();
        assertThat(resolver.identifySource()).isEqualTo(Source.DATA_MART);
        assertThat(resolver.getTableName()).isEqualTo("top_buyers");
    }

    @Test
    void testResolve_shouldThrowExceptionForInvalidBigQueryTableFormat() {
        String invalidBigQueryTable = "invalid_table_format";

        assertThatThrownBy(() -> BigQueryTableResolver.resolve(invalidBigQueryTable))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid BigQuery table format: `invalid_table_format`!");
    }

    @Test
    void testResolve_shouldThrowExceptionForUnknownSource() {
        String unknownSourceDataset = "my-dataset_1.my_table-2024";
        BigQueryTableResolver resolver = BigQueryTableResolver.resolve(unknownSourceDataset);

        assertThatThrownBy(resolver::identifySource)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unable to determine source for dataset: `my-dataset_1`!");
    }
}
