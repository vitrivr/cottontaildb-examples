package org.vitrivr.cottontail.examples
import com.google.protobuf.Empty
import io.grpc.ManagedChannelBuilder
import org.vitrivr.cottontail.grpc.*
import org.vitrivr.cottontail.grpc.CottontailGrpc.Expression
import org.vitrivr.cottontail.grpc.CottontailGrpc.Expressions
import org.vitrivr.cottontail.grpc.CottontailGrpc.InsertMessage
import org.vitrivr.cottontail.grpc.CottontailGrpc.Literal
import org.vitrivr.cottontail.utilities.VectorUtility
import java.io.BufferedReader
import java.io.InputStreamReader

/**
 * Example code for the use of Cottontail DB gRCP client library in Kotlin.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
object ExamplesGrpc {

    /** Cottontail DB gRPC channel; adjust Cottontail DB host and port according to your needs. */
    private val channel = ManagedChannelBuilder.forAddress("127.0.0.1", 1865).usePlaintext().build()

    /** Cottontail DB Stub for DDL operations (e.g. create a new Schema or Entity). */
    private val ddlService = DDLGrpc.newBlockingStub(channel)

    /** Cottontail DB Stub for DML operations (i.e. inserting Data). */
    private val dmlService = DMLGrpc.newBlockingStub(channel)

    /** Cottontail DB Stub for DQL operations (i.e. issuing queries).*/
    private val dqlService = DQLGrpc.newBlockingStub(channel)

    /** Cottontail DB Stub for transaction management.*/
    private val txnService = TXNGrpc.newBlockingStub(channel)

    /** Name of the Cottontail DB Schema. */
    private val schema_name = "cottontail_example"

    /** Name of the Cottontail DB Schema and dimension of its vector column. */
    private val entities = arrayOf(
        "scalablecolor" to 64,
        "cedd" to 144,
        "jhist" to 576
    )

    /**
     * Creates a Cottontail DB schema named "cottontail_example" using the DDL Stub.
     */
    fun initializeSchema() {
        val schemaDefinitionMessage = CottontailGrpc.CreateSchemaMessage.newBuilder()
            .setSchema(CottontailGrpc.SchemaName.newBuilder().setName(schema_name)).build()
        ddlService.createSchema(schemaDefinitionMessage)
        println("Schema $schema_name created successfully.")
    }

    /**
     * Drops a Cottontail DB schema named "cottontail_example" and all entities it contains using the DDL Stub.
     */
    fun dropSchema() {
        val schemaDefinitionMessage = CottontailGrpc.DropSchemaMessage.newBuilder()
            .setSchema(CottontailGrpc.SchemaName.newBuilder().setName(schema_name)).build()
        ddlService.dropSchema(schemaDefinitionMessage)
        println("Schema $schema_name dropped successfully.")
    }

    /**
     * Creates three entities using the DDL Stub.
     */
    fun initializeEntities() = entities.forEach {
        val definition = CottontailGrpc.EntityDefinition.newBuilder()
            .setEntity(
                CottontailGrpc.EntityName.newBuilder().setName(it.first)
                    .setSchema(CottontailGrpc.SchemaName.newBuilder().setName(schema_name))
            ) /* Name of entity and schema it belongs to. */
            .addColumns(
                CottontailGrpc.ColumnDefinition.newBuilder().setType(CottontailGrpc.Type.STRING)
                    .setEngine(CottontailGrpc.Engine.MAPDB)
                    .setName(CottontailGrpc.ColumnName.newBuilder().setName("id")).setNullable(false)
            ) /* 1st column: id (String) */
            .addColumns(
                CottontailGrpc.ColumnDefinition.newBuilder().setType(CottontailGrpc.Type.FLOAT_VEC)
                    .setEngine(CottontailGrpc.Engine.MAPDB)
                    .setName(CottontailGrpc.ColumnName.newBuilder().setName("feature")).setNullable(false)
                    .setLength(it.second)
            ) /* 2nd column: feature (float vector of given dimension). */
            .build()

        ddlService.createEntity(CottontailGrpc.CreateEntityMessage.newBuilder().setDefinition(definition).build())
        println("Entity $schema_name.${it.first} created successfully.")
    }

    /**
     * Imports the example data contained in the resource bundle of the project.
     */
    fun importData() = entities.forEach {
        /* Start a transaction per INSERT. */
        val txId = txnService.begin(Empty.getDefaultInstance())

        /* Load data from file (in resources folder). */
        val classloader = Thread.currentThread().contextClassLoader
        try {
            BufferedReader(InputStreamReader(classloader.getResourceAsStream(it.first))).useLines { lines ->
                lines.forEach { l ->
                    val split = l.split('\t')

                    /* Prepare data for first (id) column. */
                    val id = CottontailGrpc.Literal.newBuilder().setStringData(split[0]).build()

                    /* Prepare data for second (feature) column. */
                    val vector = CottontailGrpc.FloatVector.newBuilder()
                    for (e in split[3].split(' ')) {
                        vector.addVector(e.toFloat())
                    }
                    val feature = CottontailGrpc.Literal.newBuilder()
                        .setVectorData(CottontailGrpc.Vector.newBuilder().setFloatVector(vector)).build()

                    /* Prepare INSERT message. */
                    val insertMessage = InsertMessage.newBuilder()
                        .setFrom(
                            CottontailGrpc.From.newBuilder().setScan(
                                CottontailGrpc.Scan.newBuilder().setEntity(
                                    CottontailGrpc.EntityName.newBuilder().setName(it.first)
                                        .setSchema(CottontailGrpc.SchemaName.newBuilder().setName(schema_name))
                                )
                            )
                        ) /* Entity the data should be inserted into. */
                        .addElements(
                            InsertMessage.InsertElement.newBuilder()
                                .setColumn(CottontailGrpc.ColumnName.newBuilder().setName("id")).setValue(id).build()
                        )
                        .addElements(
                            InsertMessage.InsertElement.newBuilder()
                                .setColumn(CottontailGrpc.ColumnName.newBuilder().setName("feature")).setValue(feature)
                                .build()
                        )
                        .build()

                    /* Send INSERT message. */
                    dmlService.insert(insertMessage);
                }
            }
            txnService.commit(txId)
        } catch (e: Throwable) {
            println("Exception during data import.")
            txnService.rollback(txId)
            e.printStackTrace()
        }

    }

    /**
     * Select and display top 3 entries in each entity.
     */
    fun executeSimpleSelect() = entities.forEach {
        /* Prepare  query. */
        val query = CottontailGrpc.QueryMessage.newBuilder().setQuery(
            CottontailGrpc.Query.newBuilder()
                .setFrom(
                    CottontailGrpc.From.newBuilder().setScan(
                        CottontailGrpc.Scan.newBuilder().setEntity(
                            CottontailGrpc.EntityName.newBuilder().setName(it.first)
                                .setSchema(CottontailGrpc.SchemaName.newBuilder().setName(schema_name))
                        )
                    )
                ) /* Entity to select from. */
                .setProjection(
                    CottontailGrpc.Projection.newBuilder().addElements(
                        CottontailGrpc.Projection.ProjectionElement.newBuilder()
                            .setColumn(CottontailGrpc.ColumnName.newBuilder().setName("*"))
                    )
                ) /* Star projection. */
                .setLimit(3) /* Limit to top 3 entries. */
        ).build()

        /* Execute query. */
        val results = dqlService.query(query)

        /* Print results. */
        println("Results of query for entity '${it.first}':")
        results.forEach { r -> r.tuplesList.forEach { t -> println(t) } }
    }

    /**
     * Select one entry per entity based on a WHERE-clause.
     */
    fun executeSelectWithWhere() = entities.forEach {
        /* Prepare  query. */
        val query = CottontailGrpc.QueryMessage.newBuilder().setQuery(
            CottontailGrpc.Query.newBuilder()
                .setFrom(
                    CottontailGrpc.From.newBuilder().setScan(
                        CottontailGrpc.Scan.newBuilder().setEntity(
                            CottontailGrpc.EntityName.newBuilder().setName(it.first)
                                .setSchema(CottontailGrpc.SchemaName.newBuilder().setName(schema_name))
                        )
                    )
                ) /* Entity to select from. */
                .setWhere(
                    CottontailGrpc.Where.newBuilder()
                        .setAtomic( /* Predicate (WHERE-clause). Should have one match in each entry. */
                            CottontailGrpc.AtomicBooleanPredicate.newBuilder()
                                .setLeft(CottontailGrpc.ColumnName.newBuilder().setName("id").build())
                                .setOp(CottontailGrpc.ComparisonOperator.IN)
                                .setRight(
                                    CottontailGrpc.AtomicBooleanOperand.newBuilder().setExpressions(
                                        Expressions.newBuilder()
                                            .addExpression(
                                                CottontailGrpc.Expression.newBuilder().setLiteral(
                                                    CottontailGrpc.Literal.newBuilder()
                                                        .setStringData("fca0132f519e71d13fb82b86964872")
                                                )
                                            ) /* matches cedd */
                                            .addExpression(
                                                CottontailGrpc.Expression.newBuilder().setLiteral(
                                                    CottontailGrpc.Literal.newBuilder()
                                                        .setStringData("0b414f0e6e82cd0aefae3d2bd791b2")
                                                )
                                            ) /* matches jhist */
                                            .addExpression(
                                                CottontailGrpc.Expression.newBuilder().setLiteral(
                                                    CottontailGrpc.Literal.newBuilder()
                                                        .setStringData("0f412c5bd41f9b91d8635bb1a886a36")
                                                )
                                            ) /* matches scalablecolor */
                                    )
                                )
                        )
                )
                .setProjection(
                    CottontailGrpc.Projection.newBuilder().addElements(
                        CottontailGrpc.Projection.ProjectionElement.newBuilder()
                            .setColumn(CottontailGrpc.ColumnName.newBuilder().setName("*"))
                    )
                ) /* Star projection. */
        ).build()

        /* Execute query. */
        val results = dqlService.query(query)

        /* Print results. */
        println("Results of query for entity '${it.first}':")
        results.forEach { r -> r.tuplesList.forEach { t -> println(t) } }
    }


    /**
     * Executes a kNN query on each of the example entities.
     */
    fun executeNearestNeighbourQuery() = entities.forEach {
        /* Number of entries to return. */
        val k = 10

        /* Prepare query vector. */
        val vector = CottontailGrpc.FloatVector.newBuilder()
        VectorUtility.randomFloatVector(it.second).forEach { v -> vector.addVector(v) }

        /* Prepare kNN query vector. */
        val query = CottontailGrpc.QueryMessage.newBuilder().setQuery(
            CottontailGrpc.Query.newBuilder()
                .setFrom(
                    CottontailGrpc.From.newBuilder().setScan(
                        CottontailGrpc.Scan.newBuilder().setEntity(
                            CottontailGrpc.EntityName.newBuilder().setName(it.first)
                                .setSchema(CottontailGrpc.SchemaName.newBuilder().setName(schema_name))
                        )
                    )
                ) /* Entity to select from. */
                .setProjection(
                    CottontailGrpc.Projection.newBuilder()
                        .addElements(
                            CottontailGrpc.Projection.ProjectionElement.newBuilder()
                                .setColumn(CottontailGrpc.ColumnName.newBuilder().setName("id"))
                        ) /* Projection: Id column */
                        .addElements(
                            CottontailGrpc.Projection.ProjectionElement.newBuilder().setAlias(
                                CottontailGrpc.ColumnName.newBuilder().setName("distance")
                            ) /* Projection: Execute distance function --> store in alias. */
                                .setFunction(
                                    CottontailGrpc.Function.newBuilder()
                                        .setName(CottontailGrpc.FunctionName.newBuilder().setName("euclidean"))
                                        .addArguments(
                                            Expression.newBuilder().setLiteral(
                                                Literal.newBuilder().setVectorData(
                                                    CottontailGrpc.Vector.newBuilder().setFloatVector(vector)
                                                )
                                            )
                                        )
                                        .addArguments(
                                            Expression.newBuilder()
                                                .setColumn(CottontailGrpc.ColumnName.newBuilder().setName("feature"))
                                        )
                                )
                        )
                )
                .setOrder(
                    CottontailGrpc.Order.newBuilder().addComponents(
                        CottontailGrpc.Order.Component.newBuilder()
                            .setColumn(CottontailGrpc.ColumnName.newBuilder().setName("distance"))
                            .setDirection(CottontailGrpc.Order.Direction.ASCENDING)
                    )
                )
                .setLimit(k.toLong())
        ).build()

        /* Execute query. */
        val results = dqlService.query(query)

        /* Print results. */
        println("Results of kNN query for entity '${it.first}' (k = $k, column = 'feature'):")
        results.forEach { r -> r.tuplesList.forEach { t -> println(t) } }
    }
}

/**
 * Entry point for example program.
 */
fun main() {
    ExamplesGrpc.initializeSchema() /* Initialize empty schema ''. */

    ExamplesGrpc.initializeEntities() /* Initialize empty entities. */

    ExamplesGrpc.importData() /* Import example data from resource bundle. */

    ExamplesGrpc.executeSimpleSelect() /* Execute simple SELECT statement with LIMIT. */

    ExamplesGrpc.executeSelectWithWhere() /* Execute simple SELECT statement with WHERE-clause. */

    ExamplesGrpc.executeNearestNeighbourQuery() /* Execute kNN query. */
}