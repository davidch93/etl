package com.github.davidch93.etl.core.pipelines;

/**
 * An interface that defines the structure of an ETL (Extract, Transform, Load) pipeline.
 * <p>
 * This interface provides methods to prepare and execute an ETL pipeline, enabling
 * a consistent approach for implementing ETL processes. It also includes a default method
 * to streamline the execution of the preparation and startup phases in sequence.
 * </p>
 *
 * <h2>Usage:</h2>
 * <p>
 * Classes implementing this interface should define the logic for the following methods:
 * <ul>
 *   <li>{@code prepare()}: Handles any pre-execution setup tasks, such as initializing
 *   resources or validating configurations.</li>
 *   <li>{@code start()}: Executes the ETL process, such as extracting data, transforming it,
 *   and loading it into the target destination.</li>
 * </ul>
 * </p>
 *
 * <p>The {@code prepareAndStart()} method provides a convenient way to invoke both preparation
 * and execution in sequence.</p>
 *
 * <h2>Example:</h2>
 * <pre>
 * public class MyEtlPipeline implements EtlPipeline {
 *     {@code @Override}
 *     public void prepare() {
 *         // Initialize resources, validate configurations, etc.
 *     }
 *
 *     {@code @Override}
 *     public void start() {
 *         // Perform data extraction, transformation, and loading.
 *     }
 * }
 *
 * MyEtlPipeline pipeline = new MyEtlPipeline();
 * pipeline.prepareAndStart();
 * </pre>
 *
 * <h2>Thread Safety:</h2>
 * <p>
 * Implementing classes should ensure thread safety if the pipeline is expected to run
 * in a multithreaded environment.
 * </p>
 *
 * @author david.christianto
 */
public interface EtlPipeline {

    /**
     * Prepares the ETL pipeline for execution.
     * <p>
     * This method is responsible for handling pre-execution tasks, such as initializing
     * resources, validating configurations, or setting up dependencies required for the pipeline.
     * </p>
     */
    void prepare();

    /**
     * Starts the execution of the ETL pipeline.
     * <p>
     * This method performs the core tasks of the ETL process, including data extraction,
     * transformation, and loading into the target destination.
     * </p>
     */
    void start();

    /**
     * A default method to prepare and start the ETL pipeline in sequence.
     * <p>
     * This method simplifies the pipeline execution by invoking {@code prepare()}
     * and {@code start()} in the correct order.
     * </p>
     */
    default void prepareAndStart() {
        prepare();
        start();
    }
}