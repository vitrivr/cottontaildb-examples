package org.vitrivr.cottontail.examples

import io.grpc.ManagedChannelBuilder
import io.grpc.stub.StreamObserver
import org.vitrivr.cottontail.grpc.CottonDDLGrpc
import org.vitrivr.cottontail.grpc.CottonDMLGrpc
import org.vitrivr.cottontail.grpc.CottonDQLGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.utilities.VectorUtility
import java.io.BufferedReader
import java.io.InputStreamReader
import kotlin.time.ExperimentalTime


/** Cottontail DB gRPC channel; adjust Cottontail DB host and port according to your needs. */
private val channel = ManagedChannelBuilder.forAddress("127.0.0.1", 1865).usePlaintext().build()

/** Cottontail DB Stub for DDL operations (e.g. create a new Schema or Entity). */
private val ddlService = CottonDDLGrpc.newBlockingStub(channel)

/** Cottontail DB Stub for DML operations (i.e. inserting Data). */
private val dmlService = CottonDMLGrpc.newStub(channel)

/** Cottontail DB Stub for DQL operations (i.e. issuing queries).*/
private val dqlService = CottonDQLGrpc.newBlockingStub(channel)

/** Name of the Cottontail DB Schema. */
val schema_name = "cottontail_example"

/** Name of the Cottontail DB Schema and dimension of its vector column. */
val entities = arrayOf(
    "scalablecolor" to 64,
    "cedd" to 144,
    "jhist" to 576
)

/**
 * Creates a Cottontail DB schema named "cottontail_example" using the DDL Stub.
 */
fun initializeSchema() {
    val schemaDefinitionMessage = CottontailGrpc.Schema.newBuilder().setName(schema_name).build()
    ddlService.createSchema(schemaDefinitionMessage)
    println("Schema $schema_name created successfully.")
}

/**
 * Drops a Cottontail DB schema named "cottontail_example" and all entities it contains using the DDL Stub.
 */
fun dropSchema() {
    val schemaDefinitionMessage = CottontailGrpc.Schema.newBuilder().setName(schema_name).build()
    ddlService.dropSchema(schemaDefinitionMessage)
    println("Schema $schema_name dropped successfully.")
}

/**
 * Creates three entities using the DDL Stub.
 */
fun initializeEntities() = entities.forEach {
    val entityDefinitionMessage = CottontailGrpc.EntityDefinition.newBuilder()
        .setEntity(CottontailGrpc.Entity.newBuilder().setName(it.first).setSchema(CottontailGrpc.Schema.newBuilder().setName(schema_name))) /* Name of entity and schema it belongs to. */
        .addColumns(CottontailGrpc.ColumnDefinition.newBuilder().setType(CottontailGrpc.Type.STRING).setName("id").setNullable(false)) /* 1st column: id (String) */
        .addColumns(CottontailGrpc.ColumnDefinition.newBuilder().setType(CottontailGrpc.Type.FLOAT_VEC).setName("feature").setNullable(false).setLength(it.second)) /* 2nd column: feature (float vector of given dimension). */
        .build()

    ddlService.createEntity(entityDefinitionMessage)
    println("Entity $schema_name.${it.first} created successfully.")
}

/**
 * Imports the example data contained in the resource bundle of the project.
 */
fun importData() = entities.forEach {
    var complete = false
    var counter = 0

    /* Prepare stub for INSERT. */
    val stub = dmlService.insert(object: StreamObserver<CottontailGrpc.InsertStatus> {
        override fun onNext(value: CottontailGrpc.InsertStatus?) {
            counter += 1
        }

        override fun onCompleted() {
            println("Import of $counter features for ${it.first} completed! Everything committed...")
            complete = true
        }

        override fun onError(t: Throwable?) {
            println("Error occurred while importing features for ${it.first}: ${t?.message}")
            complete = true
        }
    })

    /* Load data from file (in resources folder). */
    val classloader = Thread.currentThread().contextClassLoader
    BufferedReader(InputStreamReader(classloader.getResourceAsStream(it.first))).useLines { lines ->
        lines.forEach { l ->
            val split = l.split('\t')

            /* Prepare data for first (id) column. */
            val id = CottontailGrpc.Data.newBuilder().setStringData(split[0]).build()

            /* Prepare data for second (feature) column. */
            val vector = CottontailGrpc.FloatVector.newBuilder()
            for (e in split[3].split(' ')) {
                vector.addVector(e.toFloat())
            }
            val feature = CottontailGrpc.Data.newBuilder().setVectorData(CottontailGrpc.Vector.newBuilder().setFloatVector(vector)).build()

            /* Prepare INSERT message. */
            val insertMessage = CottontailGrpc.InsertMessage.newBuilder()
                .setEntity(CottontailGrpc.Entity.newBuilder().setName(it.first).setSchema(CottontailGrpc.Schema.newBuilder().setName(schema_name))) /* Entity the data should be inserted into. */
                .setTuple(CottontailGrpc.Tuple.newBuilder()
                    .putData("id", id) /* Data for first (id) column. */
                    .putData("feature", feature) /* Data for second (feature) column. */
                )
                .build()

            /* Send INSERT message. */
            stub.onNext(insertMessage)
        }
    }

    /* Commit message. */
    stub.onCompleted()

    /* Wait for transaction to complete! */
    while (!complete) {
        Thread.yield()
    }
}

/**
 * Select and display top 3 entries in each entity.
 */
fun executeSimpleSelect() = entities.forEach {
    /* Prepare  query. */
    val query = CottontailGrpc.QueryMessage.newBuilder().setQuery(
        CottontailGrpc.Query.newBuilder()
            .setFrom(CottontailGrpc.From.newBuilder().setEntity(CottontailGrpc.Entity.newBuilder().setName(it.first).setSchema(CottontailGrpc.Schema.newBuilder().setName(schema_name)))) /* Entity to select data from. */
            .setProjection(CottontailGrpc.Projection.newBuilder().putAttributes("*", "")) /* Star projection. */
            .setLimit(3) /* Limit to top 3 entries. */
    ).build()

    /* Execute query. */
    val results = dqlService.query(query)

    /* Print results. */
    println("Results of query for entity '${it.first}':")
    results.forEach { r -> r.resultsList.forEach { t -> println(t) }}
}

/**
 * Select one entry per entity based on a WHERE-clause.
 */
fun executeSelectWithWhere() = entities.forEach {
    /* Prepare  query. */
    val query = CottontailGrpc.QueryMessage.newBuilder().setQuery(
        CottontailGrpc.Query.newBuilder()
            .setFrom(CottontailGrpc.From.newBuilder().setEntity(CottontailGrpc.Entity.newBuilder().setName(it.first).setSchema(CottontailGrpc.Schema.newBuilder().setName(schema_name)))) /* Entity to select data from. */
            .setWhere(CottontailGrpc.Where.newBuilder().setAtomic( /* Predicate (WHERE-clause). Should have one match in each entry. */
                CottontailGrpc.AtomicLiteralBooleanPredicate.newBuilder()
                    .setAttribute("id")
                    .setOp(CottontailGrpc.AtomicLiteralBooleanPredicate.Operator.IN)
                    .addData(CottontailGrpc.Data.newBuilder().setStringData("fca0132f519e71d13fb82b86964872").build()) /* matches cedd */
                    .addData(CottontailGrpc.Data.newBuilder().setStringData("0b414f0e6e82cd0aefae3d2bd791b2").build()) /* matches jhist */
                    .addData(CottontailGrpc.Data.newBuilder().setStringData("0f412c5bd41f9b91d8635bb1a886a36").build()) /* matches scalablecolor */
            ))
            .setProjection(CottontailGrpc.Projection.newBuilder().putAttributes("*", "")) /* Star projection. */

    ).build()

    /* Execute query. */
    val results = dqlService.query(query)

    /* Print results. */
    println("Results of query for entity '${it.first}':")
    results.forEach { r -> r.resultsList.forEach { t -> println(t) }}
}


/**
 * Executes a kNN query on each of the example entities.
 */
fun executeNearestNeighborQuery() = entities.forEach {
    /* Number of entries to return. */
    val k = 10

    /* Prepare query vector. */
    val vector = CottontailGrpc.FloatVector.newBuilder()
    VectorUtility.randomFloatVector(it.second).forEach { v -> vector.addVector(v) }

    /* Prepare kNN query vector. */
    val query = CottontailGrpc.QueryMessage.newBuilder().setQuery(
        CottontailGrpc.Query.newBuilder()
            .setFrom(CottontailGrpc.From.newBuilder().setEntity(CottontailGrpc.Entity.newBuilder().setName(it.first).setSchema(CottontailGrpc.Schema.newBuilder().setName(schema_name)))) /* Entity to select data from. */
            .setKnn(CottontailGrpc.Knn.newBuilder().setK(k).setAttribute("feature").setDistance(CottontailGrpc.Knn.Distance.L2).addQuery(CottontailGrpc.Vector.newBuilder().setFloatVector(vector))) /* kNN predicate on the column 'feature' with k = 10 and L2 distance. */
            .setProjection(CottontailGrpc.Projection.newBuilder().putAttributes("id", "").putAttributes("distance", "")) /* Selected attributes (id & calculated distance). */
    ).build()

    /* Execute query. */
    val results = dqlService.query(query)

    /* Print results. */
    println("Results of kNN query for entity '${it.first}' (k = $k, column = 'feature'):")
    results.forEach { r -> r.resultsList.forEach { t -> println(t) }}
}

/**
 * Entry point for example program.
 */
fun main() {
    initializeSchema() /* Initialize empty schema ''. */

    initializeEntities() /* Initialize empty entities. */

    importData() /* Import example data from resource bundle. */

    executeSimpleSelect() /* Execute simple SELECT statement with LIMIT. */

    executeSelectWithWhere() /* Execute simple SELECT statement with WHERE-clause. */

    executeNearestNeighborQuery() /* Execute kNN query. */
}
