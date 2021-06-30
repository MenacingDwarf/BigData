import java.math.BigInteger
import java.util.*
import java.io.File
import java.io.InputStream
import java.util.concurrent.CompletableFuture
import akka.actor.ActorSystem
import akka.actor.Props

import io.reactivex.rxjava3.core.Observable

fun countSimple() {
    val currentTime = System.currentTimeMillis()
    var sum = 0L

    val inputStream: InputStream = File(fileName).inputStream()
    inputStream.bufferedReader().forEachLine { line -> sum += countNumberFactors(BigInteger(line)) }

    val newTime = System.currentTimeMillis()
    println("Simple count return $sum in ${(newTime - currentTime)/1000.0} seconds.")
}

fun countMultithreading() {
    val currentTime = System.currentTimeMillis()
    var sum = 0L
    val lines: List<String> = File(fileName).readLines()
    val cores = Runtime.getRuntime().availableProcessors()

    for (i in (lines.indices step cores)) {
        val features: MutableList<CompletableFuture<Long>> = mutableListOf();
        val curCores = minOf(cores, lines.size-i)

        for (j in 1..curCores) {
            features.add(CompletableFuture.supplyAsync { countNumberFactors(BigInteger(lines[i+j-1])) })
        }

        for (j in 1..curCores) {
            sum += features[j-1].get()
        }
    }

    val newTime = System.currentTimeMillis()
    println("Multithreading count with $cores cores return $sum in ${(newTime - currentTime)/1000.0} seconds.")
}

fun countAkka() {
    val file = File(fileName)
    val actorSystem = ActorSystem.create("AkkaAttempt")
    val actorRef = actorSystem.actorOf(Props.create(MasterActor::class.java))
    actorRef.tell(file, actorRef)
}

fun countRxObservable() {
    val currentTime = System.currentTimeMillis()
    var array = Collections.synchronizedList(mutableListOf<Long>())
    val numbers = File(fileName).readLines().map { it.toBigInteger() }

    Observable.fromIterable(numbers)
        .map { x -> countNumberFactors(x) }
        .subscribe { x -> array.add(x) }

    val newTime = System.currentTimeMillis()
    println("RxObservable count return ${array.sum()} in ${(newTime - currentTime)/1000.0} seconds.")
}


fun main(args: Array<String>) {
    generateNumbers()
    countSimple()
    countMultithreading()
    countAkka()
    countRxObservable()
}