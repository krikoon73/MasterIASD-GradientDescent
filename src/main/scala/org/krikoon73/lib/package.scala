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

}
