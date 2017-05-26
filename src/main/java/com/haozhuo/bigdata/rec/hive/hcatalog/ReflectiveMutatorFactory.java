package com.haozhuo.bigdata.rec.hive.hcatalog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hive.hcatalog.streaming.mutate.worker.*;

import java.io.IOException;

/**
 * Created by hadoop on 5/24/17.
 */
public class ReflectiveMutatorFactory implements MutatorFactory {
    private final int recordIdColumn;
    private final ObjectInspector objectInspector;
    private final Configuration configuration;
    private final int[] bucketColumnIndexes;

    public ReflectiveMutatorFactory(Configuration configuration, Class<?> recordClass, int recordIdColumn,
                                    int[] bucketColumnIndexes) {
        this.configuration = configuration;
        this.recordIdColumn = recordIdColumn;
        this.bucketColumnIndexes = bucketColumnIndexes;
        objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(recordClass,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    public Mutator newMutator(AcidOutputFormat<?, ?> outputFormat, long transactionId, Path partitionPath, int bucketId)
            throws IOException {
        return new MutatorImpl(configuration, recordIdColumn, objectInspector, outputFormat, transactionId, partitionPath,
                bucketId);
    }

    public RecordInspector newRecordInspector() {
        return new RecordInspectorImpl(objectInspector, recordIdColumn);
    }

    public BucketIdResolver newBucketIdResolver(int totalBuckets) {
        return new BucketIdResolverImpl(objectInspector, recordIdColumn, totalBuckets, bucketColumnIndexes);
    }

}
