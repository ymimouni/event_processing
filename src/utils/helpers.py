from py4j.java_gateway import java_import


def load_csv_data(spark_session, path, schema=None, header=None):
    """
    Load CSV data.
    :return: DataFrame.
    """
    return spark_session.read.csv(path=path, schema=schema, header=header)


def save_analysis_results_to_csv(results, spark, path):
    """
    Save the analysis results in the file specified in the path argument.
    Since Spark does not allow to specify the filename, we use the Hadoop API for managing the filesystem to save the 
    output of the analysis in a temporary folder and then move it the requested path.
    """
    # Temporary folder.
    RESULTS_TEMP_FOLDER = "analysis_results_temp/"  # noqa

    # Save the analysis results in the temporary folder.
    results.coalesce(numPartitions=1) \
        .write \
        .csv(path=RESULTS_TEMP_FOLDER, mode='overwrite', header=True, emptyValue='')

    java_import(spark._jvm, 'org.apache.hadoop.fs.Path')  # noqa

    # Find out the name given to the results file by Hadoop.
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())  # noqa
    file = fs.globStatus(spark._jvm.Path(RESULTS_TEMP_FOLDER + "part*"))[0].getPath().getName()  # noqa

    # Delete if there is an older results file in the given output_folder. This is done because the API does not allow
    # to replace the file if it already exists.
    fs.delete(spark._jvm.Path(path), True)  # noqa

    # Move the results file from the temporary folder to the given path.
    fs.rename(spark._jvm.Path(RESULTS_TEMP_FOLDER + file), spark._jvm.Path(path))  # noqa

    # Delete the temporary folder.
    fs.delete(spark._jvm.Path(RESULTS_TEMP_FOLDER), True)  # noqa
