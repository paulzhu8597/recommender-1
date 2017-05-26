package com.haozhuo.bigdata.rec.hive.hcatalog;

import com.haozhuo.bigdata.rec.bean.Users;
import com.haozhuo.bigdata.rec.Props;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.streaming.mutate.client.*;
import org.apache.hive.hcatalog.streaming.mutate.worker.BucketIdResolver;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorCoordinator;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorCoordinatorBuilder;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorFactory;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class HiveClient implements Serializable{
    private String metaStoreUri;
    private String database;
    private String table;
    private MutatorFactory mutatorFactory;
    private MutatorClient client;
    private Transaction transaction;
    private AcidTable acidTable;

    public HiveClient(String metaStoreUri, String database, String table) {
        this.metaStoreUri = metaStoreUri;
        this.database = database;
        this.table = table;
        init();
    }

    public HiveClient() {
        this.metaStoreUri = Props.get("hive.metaStoreUri");
        this.database = Props.get("hive.database");
        this.table = Props.get("hive.table");
        init();
    }

    public void init() {
        try {
            client = new MutatorClientBuilder()
                    .addSinkTable(database, table, false)
                    .metaStoreUri(metaStoreUri)
                    .build();
            client.connect();
            transaction = client.newTransaction();
            List<AcidTable> tables = client.getTables();
            acidTable = tables.get(0);
            initFactory();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initFactory() {
        final int RECORD_ID_COLUMN = 2;
        final int[] BUCKET_COLUMN_INDEXES = new int[] { 0 };
        HiveStreamUtils testUtils = new HiveStreamUtils();
        HiveConf conf = testUtils.newHiveConf(metaStoreUri);
        mutatorFactory = new ReflectiveMutatorFactory(conf, Users.class, RECORD_ID_COLUMN,
                BUCKET_COLUMN_INDEXES);
    }

    public void insertData(Users[] users) {
        try {
            transaction.begin();
            BucketIdResolver bucketIdResolver = mutatorFactory.newBucketIdResolver(acidTable.getTotalBuckets());
            MutatorCoordinator coordinator = new MutatorCoordinatorBuilder()
                    .metaStoreUri(metaStoreUri)
                    .table(acidTable)
                    .mutatorFactory(mutatorFactory)
                    .build();

            for(Users user:users) {
                coordinator.insert(null, bucketIdResolver.attachBucketIdToRecord(user));
            }
            coordinator.close();
            transaction.commit();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        HiveClient client = new HiveClient();
        List<Users> users = new ArrayList<Users>();
        users.add(new Users(11,"凌鑫"));
        users.add(new Users(12, "张三"));
       // client.insertData(users);
        client.close();
    }
}
