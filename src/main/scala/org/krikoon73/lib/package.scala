package org.krikoon73

import org.krikoon73.lib.LoggerHelper
import org.apache.spark.rdd._

package object lib {
    
    def prods(x:Array[Double], y:Array[Double]) = (for(z<-0 to x.size - 1) yield (x(z)*y(z)) ).reduce( (a,b) => a+b)

    def subtr(x:Array[Double], y:Array[Double]) = ( x zip y).map( a => a._1 - a._2)

    def sum(x:Array[Double], y:Array[Double]) = ( x zip y).map( a => a._1 + a._2)

    def prodbyscal(s:Double,x:Array[Double]) = ( for(z<-0 to x.size - 1) yield (x(z)*s) ).toArray[Double]

    def BaseGradient(train : RDD[(Double, Array[Double])], stepsize : Double, n : Int, sizefeat : Int): Array[Double] = {

        val m:Double=train.count() // card(train)
        var w = Array.fill(sizefeat)(0.0)
        var grad = Array.fill(sizefeat)(0.0)
        // Global loop
        for (j <- 1 to n)
        {
            // Gradient computation
            LoggerHelper.logWarn(s"ITERATION : ${j}")
            grad = train.map{case (y,x)=> (prodbyscal(2.0*(prods(w,x)-y),x))}.reduce(sum)
            w = subtr(w,prodbyscal((stepsize/m),grad))
        }
        return w
    }
/*
    def LocalGradient(train : Array[(Double, Array[Double])],current : Array[Double],stepsize : Double, sizefeat : Int): Array[Double] = {
        val m:Double=train.count() // card(train)
        var w = Array.fill(sizefeat)(0.0)
        var temp = current 
        train.foreach{case (y,x)=> temp = sum(temp,(prodbyscal(2.0*(prods(w,x)-y),x)))
            w = subtr(w,prodbyscal((stepsize/m),temp))
        }
        return w
    }

    def SGD(train: RDD[(Double, Array[Double])],stepsize : Double, n : Int, sizefeat : Int): Array[Double] = {
        val m:Double=train.count() // card(train)
        var w = Array.fill(sizefeat)(0.0)
        val traingi = train.glom().zipWithIndex()
        val np = traingi.count().toInt
        for (j <- 1 to np)
        {
        // Gradient computation
        //var temp = Array.fill(sizefeat)(0.0)
            w = traingi.flatMap{case (p,i) => {if (i == j) {LocalGradient(p,w,stepsize,sizefeat)} else Array()}}.collect(0)
        }
        return w
    }
*/
}
