package org.vitrivr.cottontail.examples
import io.grpc.ManagedChannelBuilder
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.basics.Direction
import org.vitrivr.cottontail.client.language.basics.Distances
import org.vitrivr.cottontail.client.language.basics.Type
import org.vitrivr.cottontail.client.language.ddl.CreateEntity
import org.vitrivr.cottontail.client.language.ddl.CreateSchema
import org.vitrivr.cottontail.client.language.ddl.DropSchema
import org.vitrivr.cottontail.client.language.dml.Insert
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.client.language.extensions.Literal
import org.vitrivr.cottontail.utilities.VectorUtility
import java.io.BufferedReader
import java.io.InputStreamReader

/**
 * Example code for the use of Cottontail DB [SimpleClient] library in Kotlin.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
object ExamplesSimple {

    /** Cottontail DB gRPC channel; adjust Cottontail DB host and port according to your needs. */
    private val channel = ManagedChannelBuilder.forAddress("127.0.0.1", 1865).usePlaintext().build()

    /** Cottontail DB [SimpleClient] for all database operations. */
    private val client = SimpleClient(channel)

    /** Name of the Cottontail DB Schema. */
    private val schemaName = "cottontail_example"

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
        this.client.create(CreateSchema(schemaName))
        println("Schema $schemaName created successfully.")
    }

    /**
     * Drops a Cottontail DB schema named "cottontail_example" and all entities it contains using the DDL Stub.
     */
    fun dropSchema() {
        client.drop(DropSchema(schemaName))
        println("Schema $schemaName dropped successfully.")
    }

    /**
     * Creates three entities using the DDL Stub.
     */
    fun initializeEntities() = entities.forEach {
        this.client.create(
            CreateEntity("${this.schemaName}.${it.first}")
            .column(name = "id", type = Type.STRING, nullable = false)
            .column(name = "feature", type = Type.FLOAT_VECTOR, length = it.second, nullable = false)
        )
        println("Entity $schemaName.${it.first} created successfully.")
    }

    /**
     * Imports the example data contained in the resource bundle of the project.
     */
    fun importData() = entities.forEach {
        /* Start a transaction per INSERT. */
        val txId = this.client.begin()

        /* Load data from file (in resources folder). */
        val classloader = Thread.currentThread().contextClassLoader
        try {
            BufferedReader(InputStreamReader(classloader.getResourceAsStream(it.first))).useLines { lines ->
                lines.forEach { l ->
                    val split = l.split('\t')

                    /* Prepare data for second (feature) column. */
                    val feature = split[3].split(' ').map { v -> v.toFloat() }.toFloatArray()
                    this.client.insert(Insert("${this.schemaName}.${it.first}").value("id", split[0]).value("feature", feature));
                }
            }
            this.client.commit(txId)
        } catch (e: Throwable) {
            println("Exception during data import.")
            this.client.rollback(txId)
            e.printStackTrace()
        }

    }

    /**
     * Select and display top 3 entries in each entity.
     */
    fun executeSimpleSelect() = entities.forEach {
        /* Prepare query. */
        val query = Query("${this.schemaName}.${it.first}").select("*").limit(3)

        /* Execute query. */
        val results = this.client.query(query)

        /* Print results. */
        println("Results of query for entity '${it.first}':")
        results.forEach { t ->println(t) }
    }

    /**
     * Select one entry per entity based on a WHERE-clause.
     */
    fun executeSelectWithWhere() = entities.forEach {
        /* Prepare query. */
        val query = Query("${this.schemaName}.${it.first}").select("*").where(Literal("id", "IN", "fca0132f519e71d13fb82b86964872", "0b414f0e6e82cd0aefae3d2bd791b2", "0f412c5bd41f9b91d8635bb1a886a36"))

        /* Execute query. */
        val results = this.client.query(query)

        /* Print results. */
        println("Results of query for entity '${it.first}':")
        results.forEach { t -> println(t) }
    }


    /**
     * Executes a kNN query on each of the example entities.
     */
    fun executeNearestNeighbourQuery() = entities.forEach {
        /* Number of entries to return. */
        val k = 10L

        /* Prepare kNN query vector. */
        val vector =  VectorUtility.randomFloatVector(it.second)
        val query = Query("${this.schemaName}.${it.first}")
            .select("*")
            .distance("feature",  vector, Distances.EUCLIDEAN, "distance")
            .order("distance", Direction.ASC)
            .limit(k)


        /* Execute query. */
        val results = this.client.query(query)

        /* Print results. */
        println("Results of kNN query for entity '${it.first}' (k = $k, column = 'feature'):")
        results.forEach { t -> println(t) }
    }
}


/**
 * Entry point for example program.
 */
fun main() {
    ExamplesSimple.initializeSchema() /* Initialize empty schema ''. */

    ExamplesSimple.initializeEntities() /* Initialize empty entities. */

    ExamplesSimple.importData() /* Import example data from resource bundle. */

    ExamplesSimple.executeSimpleSelect() /* Execute simple SELECT statement with LIMIT. */

    ExamplesSimple.executeSelectWithWhere() /* Execute simple SELECT statement with WHERE-clause. */

    ExamplesSimple.executeNearestNeighbourQuery() /* Execute kNN query. */
}
