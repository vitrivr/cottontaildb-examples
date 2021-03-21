package org.vitrivr.cottontail;

import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;


import org.apache.commons.lang3.tuple.Pair;
import org.vitrivr.cottontail.grpc.*;
import org.vitrivr.cottontail.utilities.VectorUtility;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

/**
 * Example code for the use of Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
public class Examples {
    /** Cottontail DB gRPC channel; adjust Cottontail DB host and port according to your needs. */
    private static final ManagedChannel CHANNEL  = ManagedChannelBuilder.forAddress("127.0.0.1", 1865).usePlaintext().build();

    /** Cottontail DB Stub for DDL operations (e.g. create a new Schema or Entity). */
    private static final DDLGrpc.DDLBlockingStub DDL_SERVICE = DDLGrpc.newBlockingStub(CHANNEL);

    /** Cottontail DB Stub for DML operations (i.e. inserting Data). */
    private static final DMLGrpc.DMLBlockingStub DML_SERVICE = DMLGrpc.newBlockingStub(CHANNEL);

    /** Cottontail DB Stub for DQL operations (i.e. issuing queries).*/
    private static final DQLGrpc.DQLBlockingStub DQL_SERVICE = DQLGrpc.newBlockingStub(CHANNEL);

    /** Cottontail DB Stub for Transaction management.*/
    private static final TXNGrpc.TXNBlockingStub TXN_SERVICE = TXNGrpc.newBlockingStub(CHANNEL);

    /** Name of the Cottontail DB Schema. */
    private static final String SCHEMA_NAME = "cottontail_example";

    /** Name of the Cottontail DB Schema and dimension of its vector column. */
    private static final Pair<String,Integer>[] ENTITIES = new Pair[]{Pair.of("scalablecolor", 64), Pair.of("cedd", 144), Pair.of("jhist", 576)};

    /**
     * Creates a Cottontail DB schema named "cottontail_example" using the DDL Stub.
     */
    public static void initializeSchema() {
        final CottontailGrpc.CreateSchemaMessage schemaDefinitionMessage = CottontailGrpc.CreateSchemaMessage.newBuilder().setSchema(CottontailGrpc.SchemaName.newBuilder().setName(SCHEMA_NAME)).build();
        DDL_SERVICE.createSchema(schemaDefinitionMessage);
        System.out.println("Schema '" + SCHEMA_NAME + "' created successfully.");
    }

    /**
     * Drops a Cottontail DB schema named "cottontail_example" and all entities it contains using the DDL Stub.
     */
    public static void dropSchema() {
        final CottontailGrpc.DropSchemaMessage schemaDefinitionMessage = CottontailGrpc.DropSchemaMessage.newBuilder().setSchema(CottontailGrpc.SchemaName.newBuilder().setName(SCHEMA_NAME)).build();
        DDL_SERVICE.dropSchema(schemaDefinitionMessage);
        System.out.println("Schema '" + SCHEMA_NAME + "' dropped successfully.");
    }

    /**
     * Creates three entities using the DDL Stub.
     */
    public static void initializeEntities() {
        for (Pair<String,Integer> entity : ENTITIES) {
            final CottontailGrpc.EntityDefinition definition = CottontailGrpc.EntityDefinition.newBuilder()
                .setEntity(CottontailGrpc.EntityName.newBuilder().setName(entity.getLeft()).setSchema(CottontailGrpc.SchemaName.newBuilder().setName(SCHEMA_NAME))) /* Name of entity and schema it belongs to. */
                .addColumns(CottontailGrpc.ColumnDefinition.newBuilder().setType(CottontailGrpc.Type.STRING).setName("id").setEngine(CottontailGrpc.Engine.MAPDB).setNullable(false)) /* 1st column: id (String) */
                .addColumns(CottontailGrpc.ColumnDefinition.newBuilder().setType(CottontailGrpc.Type.FLOAT_VEC).setName("feature").setNullable(false).setLength(entity.getRight()))  /* 2nd column: feature (float vector of given dimension). */
                .build();

            DDL_SERVICE.createEntity(CottontailGrpc.CreateEntityMessage.newBuilder().setDefinition(definition).build());
            System.out.println("Entity '" + SCHEMA_NAME + "." + entity.getLeft() + "' created successfully.");
        }
    }

    /**
     * Imports the example data contained in the resource bundle of the project.
     */
    public static void importData() {
        for (Pair<String,Integer> entity : ENTITIES) {
            /* Start a transaction per INSERT. */
            final CottontailGrpc.TransactionId txId = TXN_SERVICE.begin(Empty.getDefaultInstance());

            /* Load data from file (in resources folder). */

            final ClassLoader classloader = Thread.currentThread().getContextClassLoader();
            try (final BufferedReader reader = new BufferedReader(new InputStreamReader(classloader.getResourceAsStream(entity.getLeft())))) {
                String l;
                while ((l = reader.readLine()) != null) {
                    final String[] split = l.split("\t");

                    /* Prepare data for first (id) column. */
                    final CottontailGrpc.Literal id = CottontailGrpc.Literal.newBuilder().setStringData(split[0]).build();

                    /* Prepare data for second (feature) column. */
                    final CottontailGrpc.FloatVector.Builder vector = CottontailGrpc.FloatVector.newBuilder();
                    for (String e : split[3].split(" ")) {
                        vector.addVector(Float.parseFloat(e));
                    }
                    final CottontailGrpc.Literal feature = CottontailGrpc.Literal.newBuilder().setVectorData(CottontailGrpc.Vector.newBuilder().setFloatVector(vector)).build();

                    /* Prepare INSERT message. */
                    final CottontailGrpc.InsertMessage insertMessage = CottontailGrpc.InsertMessage.newBuilder()
                        .setTxId(txId)
                        .setFrom(CottontailGrpc.From.newBuilder().setScan(CottontailGrpc.Scan.newBuilder().setEntity(CottontailGrpc.EntityName.newBuilder().setName(entity.getLeft()).setSchema(CottontailGrpc.SchemaName.newBuilder().setName(SCHEMA_NAME))))) /* Entity the data should be inserted into. */
                        .addInserts(CottontailGrpc.InsertMessage.InsertElement.newBuilder().setColumn(CottontailGrpc.ColumnName.newBuilder().setName("id")).setValue(id).build())
                        .addInserts(CottontailGrpc.InsertMessage.InsertElement.newBuilder().setColumn(CottontailGrpc.ColumnName.newBuilder().setName("feature")).setValue(feature).build())
                        .build();

                    /* Send INSERT message. */
                    DML_SERVICE.insert(insertMessage);
                }
                TXN_SERVICE.commit(txId);
            } catch (IOException e) {
                System.out.println("Exception during data import.");
                TXN_SERVICE.rollback(txId);
                e.printStackTrace();
            }
        }
    }

    /**
     * Select and display top 3 entries in each entity.
     */
    public static void executeSimpleSelect() {
        for (Pair<String,Integer> entity : ENTITIES) {
            /* Prepare  query. */
            final CottontailGrpc.QueryMessage query = CottontailGrpc.QueryMessage.newBuilder().setQuery(
                CottontailGrpc.Query.newBuilder().setFrom(CottontailGrpc.From.newBuilder().setScan(
                    CottontailGrpc.Scan.newBuilder().setEntity(CottontailGrpc.EntityName.newBuilder().setName(entity.getLeft()).setSchema(CottontailGrpc.SchemaName.newBuilder().setName(SCHEMA_NAME))))
                )
                .setProjection(CottontailGrpc.Projection.newBuilder().addColumns(
                        CottontailGrpc.Projection.ProjectionElement.newBuilder().setColumn(CottontailGrpc.ColumnName.newBuilder().setName("*") /* Star projection. */
                )))
                .setLimit(3) /* Limit to top 3 entries. */
            ).build();

            /* Execute query. */
            final Iterator<CottontailGrpc.QueryResponseMessage> results = DQL_SERVICE.query(query);

            /* Print results. */
            System.out.println("Results of query for entity '" + entity.getLeft() + "':");
            results.forEachRemaining(r -> r.getTuplesList().forEach(t -> System.out.println(t.toString())));
        }
    }

    /**
     * Select one entry per entity based on a WHERE-clause.
     */
    public static void executeSelectWithWhere() {
        for (Pair<String,Integer> entity : ENTITIES) {
            /* Prepare  query. */
            final CottontailGrpc.QueryMessage query = CottontailGrpc.QueryMessage.newBuilder().setQuery(
                CottontailGrpc.Query.newBuilder().setFrom(CottontailGrpc.From.newBuilder().setScan(
                    CottontailGrpc.Scan.newBuilder().setEntity(CottontailGrpc.EntityName.newBuilder().setName(entity.getLeft()).setSchema(CottontailGrpc.SchemaName.newBuilder().setName(SCHEMA_NAME)))).build() /* Entity to select data from. */
                )
                .setWhere(CottontailGrpc.Where.newBuilder().setAtomic( /* Predicate (WHERE-clause). Should have one match in each entry. */
                    CottontailGrpc.AtomicBooleanPredicate.newBuilder()
                        .setLeft(CottontailGrpc.ColumnName.newBuilder().setName("id").build())
                        .setOp(CottontailGrpc.ComparisonOperator.IN)
                        .setRight(CottontailGrpc.AtomicBooleanOperand.newBuilder().setLiterals(
                                CottontailGrpc.Literals.newBuilder()
                                    .addLiteral(CottontailGrpc.Literal.newBuilder().setStringData("fca0132f519e71d13fb82b86964872").build()) /* matches cedd */
                                    .addLiteral(CottontailGrpc.Literal.newBuilder().setStringData("0b414f0e6e82cd0aefae3d2bd791b2").build()) /* matches jhist */
                                    .addLiteral(CottontailGrpc.Literal.newBuilder().setStringData("0f412c5bd41f9b91d8635bb1a886a36").build()) /* matches scalablecolor */
                        ))
                ))
                .setProjection(CottontailGrpc.Projection.newBuilder().addColumns(
                    CottontailGrpc.Projection.ProjectionElement.newBuilder().setColumn(CottontailGrpc.ColumnName.newBuilder().setName("*") /* Star projection. */
                )))
            ).build();

            /* Execute query. */
            final Iterator<CottontailGrpc.QueryResponseMessage> results = DQL_SERVICE.query(query);

            /* Print results. */
            System.out.println("Results of query for entity '" + entity.getLeft() + "':");
            results.forEachRemaining(r -> r.getTuplesList().forEach(t -> System.out.println(t.toString())));
        }
    }

    /**
     * Executes a kNN query on each of the example entities.
     */
    public static void  executeNearestNeighborQuery()  {
        /* Number of entries to return. */
        final int k = 10;

        for (Pair<String,Integer> entity : ENTITIES) {
            /* Prepare query vector. */
            final CottontailGrpc.FloatVector.Builder vector = CottontailGrpc.FloatVector.newBuilder();
            for (float v : VectorUtility.INSTANCE.randomFloatVector(entity.getRight())) {
                vector.addVector(v);
            }

            /* Prepare kNN query vector. */
            final CottontailGrpc.QueryMessage query = CottontailGrpc.QueryMessage.newBuilder().setQuery(
                CottontailGrpc.Query.newBuilder().setFrom(CottontailGrpc.From.newBuilder().setScan(
                    CottontailGrpc.Scan.newBuilder().setEntity(CottontailGrpc.EntityName.newBuilder().setName(entity.getLeft()).setSchema(CottontailGrpc.SchemaName.newBuilder().setName(SCHEMA_NAME)))).build() /* Entity to select data from. */
                )
                .setKnn(CottontailGrpc.Knn.newBuilder().setK(k).setAttribute(CottontailGrpc.ColumnName.newBuilder().setName("feature")).setDistance(CottontailGrpc.Knn.Distance.L2).setQuery(CottontailGrpc.Vector.newBuilder().setFloatVector(vector))) /* kNN predicate on the column 'feature' with k = 10 and L2 distance. */
                .setProjection(CottontailGrpc.Projection.newBuilder().addColumns(
                  CottontailGrpc.Projection.ProjectionElement.newBuilder().setColumn(CottontailGrpc.ColumnName.newBuilder().setName("id")) /* Star projection. */
                ).addColumns(
                  CottontailGrpc.Projection.ProjectionElement.newBuilder().setColumn(CottontailGrpc.ColumnName.newBuilder().setName("distance")) /* Star projection. */
                ))
            ).build();

            /* Execute query. */
            final Iterator<CottontailGrpc.QueryResponseMessage> results = DQL_SERVICE.query(query);

            /* Print results. */
            System.out.println("Results of query for entity '" + entity.getLeft() + "':");
            results.forEachRemaining(r -> r.getTuplesList().forEach(t -> System.out.println(t.toString())));
        }
    }

    /**
     * Entry point for example program.
     *
     * @param args
     */
    public static void main(String[] args) {
        initializeSchema(); /* Initialize empty schema ''. */

        initializeEntities(); /* Initialize empty entities. */

        importData(); /* Import example data from resource bundle. */

        executeSimpleSelect(); /* Execute simple SELECT statement with LIMIT. */

        executeSelectWithWhere(); /* Execute simple SELECT statement with WHERE-clause. */

        executeNearestNeighborQuery(); /* Execute kNN query. */
    }
}

