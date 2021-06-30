import akka.actor.AbstractActor
import akka.actor.Props
import akka.japi.pf.ReceiveBuilder
import java.io.File
import java.math.BigInteger
import java.util.*

// ------------- NUMBERS GENERATION------------- //

const val fileName = "big_numbers.txt"

fun generateNumbers () {
    val random = Random()
    val nums = arrayListOf<BigInteger>()
    for (i in (0..1999)) {
        nums.add(BigInteger(48, random))
    }
    File(fileName).writeText(nums.joinToString("\n"))
}

// ------------- FACTORS COUNT ------------- //

fun countNumberFactors(bigInteger: BigInteger): Long {
    val THREE: BigInteger = BigInteger.valueOf(3L)

    if (bigInteger < BigInteger.TWO) return 0
    if (bigInteger.isProbablePrime(20)) {
        return 1L
    }

    var factorsCount = 0L
    var factor = BigInteger.TWO
    var current = bigInteger
    while (true) {
        if (current % factor == BigInteger.ZERO) {
            factorsCount += 1
            current /= factor
            if (current == BigInteger.ONE) return factorsCount
            if (current.isProbablePrime(20)) factor = current
        } else if (factor >= THREE) factor += BigInteger.TWO
        else factor = THREE
    }
}

// ------------- FOR AKKA ------------- //

data class CountFactors(val countFactors: Long)

class MasterActor : AbstractActor() {
    var workersCount = 0
    var finished = 0
    var currentTime = 0L
    var count = 0L

    override fun createReceive() =
        ReceiveBuilder()
            .match(File::class.java) { file ->
                currentTime = System.currentTimeMillis()

                val numbers = file.readLines().map { BigInteger(it) }
                workersCount = numbers.size
                val workers = Array(workersCount) { context.actorOf(Props.create(SlaveActor::class.java)) }

                for (i in 0 until workersCount) {
                    workers[i].tell(numbers[i], self)
                }
            }
            .match(CountFactors::class.java) {
                count += it.countFactors
                finished++
                if (finished == workersCount) {
                    val newTime = System.currentTimeMillis()
                    println("Akka count return $count in ${(newTime - currentTime)/1000.0} seconds.")
                    context.system.terminate()
                }
            }
            .build()!!
}

class SlaveActor : AbstractActor() {
    override fun createReceive() =
        ReceiveBuilder()
            .match(BigInteger::class.java) {
                val countFactors = countNumberFactors(it)
                sender.tell(CountFactors(countFactors), self)}
            .build()!!
}