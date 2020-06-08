package org.vitrivr.cottontail.utilities

import java.util.*

/**
 * Utility class that can be used to generate random vectors and vector sequences.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object VectorUtility {

    /** The random number generator used for vector generation. */
    private val random = Random()

    /**
     * Generates a random [IntArray] of the given size.
     *
     * @param size The size of the random vector.
     */
    fun randomIntVector(size: Int) : IntArray {
        val vec = IntArray(size)
        for (i in 0 until vec.size) {
            vec[i] = random.nextInt()
        }
        return vec
    }

    /**
     * Generates a random [LongArray] of the given size.
     *
     * @param size The size of the random vector.
     */
    fun randomLongVector(size: Int) : LongArray {
        val vec = LongArray(size)
        for (i in 0 until vec.size) {
            vec[i] = random.nextLong()
        }
        return vec
    }

    /**
     * Generates a random [FloatArray] of the given size.
     *
     * @param size The size of the random vector.
     */
    fun randomFloatVector(size: Int) : FloatArray {
        val vec = FloatArray(size)
        for (i in vec.indices) {
            vec[i] = random.nextFloat()
        }
        return vec
    }

    /**
     * Generates a random [DoubleVectorValue] of the given size.
     *
     * @param size The size of the random vector.
     */
    fun randomDoubleVector(size: Int) : DoubleArray {
        val vec = DoubleArray(size)
        for (i in vec.indices) {
            vec[i] = random.nextDouble()
        }
        return vec
    }

    /**
     * Generates a sequence of random [IntArray] of the given size.
     *
     * @param size The size of the random vectors.
     * @param items The number of items to return from the [Iterator]
     */
    fun randomIntVectorSequence(size: Int, items: Int = Int.MAX_VALUE) : Iterator<IntArray> = object: Iterator<IntArray> {
        var left = items
        override fun hasNext(): Boolean = this.left > 0
        override fun next(): IntArray {
            this.left -= 1
            return randomIntVector(size)
        }
    }

    /**
     * Generates a sequence of random [FloatArray] of the given size.
     *
     * @param size The size of the random vectors.
     * @param items The number of items to return from the [Iterator]
     */
    fun randomLongVectorSequence(size: Int, items: Int = Int.MAX_VALUE) : Iterator<LongArray> = object: Iterator<LongArray> {
        var left = items
        override fun hasNext(): Boolean = this.left > 0
        override fun next(): LongArray {
            this.left -= 1
            return randomLongVector(size)
        }
    }

    /**
     * Generates a sequence of random [FloatVectorValue] of the given size.
     *
     * @param size The size of the random vectors.
     * @param items The number of items to return from the [Iterator]
     */
    fun randomFloatVectorSequence(size: Int, items: Int = Int.MAX_VALUE) : Iterator<FloatArray> = object: Iterator<FloatArray> {
        var left = items
        override fun hasNext(): Boolean = this.left > 0
        override fun next(): FloatArray {
            this.left -= 1
            return randomFloatVector(size)
        }
    }

    /**
     * Generates a sequence of random [DoubleVectorValue] of the given size.
     *
     * @param size The size of the random vectors.
     * @param items The number of items to return from the [Iterator]
     */
    fun randomDoubleVectorSequence(size: Int, items: Int = Int.MAX_VALUE) : Iterator<DoubleArray> = object: Iterator<DoubleArray> {
        var left = items
        override fun hasNext(): Boolean = this.left > 0
        override fun next(): DoubleArray {
            this.left -= 1
            return randomDoubleVector(size)
        }
    }
}