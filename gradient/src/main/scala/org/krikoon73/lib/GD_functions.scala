package org.krikoon73

import org.krikoon73.lib.LoggerHelper
import org.apache.spark.rdd._

package object lib {

    // Scalar product of 2 vectors
    def prods(x:Array[Double], y:Array[Double]): Double = (for(z<-0 to x.size - 1) yield (x(z)*y(z)) ).reduce((a, b) => a+b)

    // Subtraction of 2 vectors
    def subtr(x:Array[Double], y:Array[Double]): Array[Double] = ( x zip y).map(a => a._1 - a._2)

    // Addition of 2 vectors
    def sum(x:Array[Double], y:Array[Double]): Array[Double] = ( x zip y).map(a => a._1 + a._2)

    // Product of vector by a scalar
    def prodbyscal(s:Double,x:Array[Double]): Array[Double] = ( for(z<-0 to x.size - 1) yield (x(z)*s) ).toArray[Double]

    // batch GD

    def sigma(train : Array[(Double,Array[Double])], current_w : Array[Double], sizefeat : Int): Array[Double] = {
        var sigma = Array.fill(sizefeat)(0.0)
        val m:Double=train.length
        train.foreach{case (y,x) =>
            sigma = sum(sigma,prodbyscal(2.0*(prods(current_w,x)-y),x))
        }
      sigma
    }

    //def batchGD(dtrain : Array[(Double, Array[Double])],init_w : Array[Double],  sizefeat : Int , stepsize : Double, nb_of_epochs: Int): Array[Double] =
    def batchGD(dtrain : Array[(Double, Array[Double])], sizefeat : Int , stepsize : Double, nb_of_epochs: Int): Array[Double] =
    {
        //var w = init_w
        var w = Array.fill(sizefeat)(0.0)
        val m:Double=dtrain.length
        for (i <- 1 to nb_of_epochs){
            val s = sigma(dtrain, w, sizefeat) ;
            w = subtr(w,prodbyscal((stepsize/m),s))
        }
        w
    }

    def SGD(dtrain : Array[(Double, Array[Double])], init_w : Array[Double] , stepsize: Double): Array[Double] =
    {
        var w = init_w ;
        for (d<-dtrain){val y=d._1; val x=d._2; w= subtr(w, prodbyscal(stepsize*2.0*(prods(w,x)-y),x))} ;
        w
    }

    def SGD_momentum(dtrain : Array[(Double, Array[Double])], init_w : Array[Double] , stepsize: Double, init_v:Array[Double], momentum_term:Double): (Array[Double], Array[Double]) =
    {
        var w = init_w ;
        var v = init_v ;
        for (d<-dtrain){val y=d._1; val x=d._2; v=sum(prodbyscal(momentum_term,v), prodbyscal(stepsize*2.0*(prods(w,x)-y),x))  ;   w= subtr(w, v)} ;
        (w,v)
    }

    // batch GD with RDDs

    def sigma_RDD(train : RDD[(Double, Array[Double])],current_w : Array[Double]): Array[Double] = {
        train.map{case (y,x)=>
            (prodbyscal(2.0*(prods(current_w,x)-y),x))
        }.reduce(sum)
    }

    def batchGD_RDD(dtrain : RDD[(Double, Array[Double])],
                    stepsize : Double,
                    nb_of_epochs: Int,
                    sizefeat : Int ,
                    m: Int): Array[Double] =
    {
        var w= Array.fill(sizefeat)(0.0)
        for (i <- 1 to nb_of_epochs){
            val s = sigma_RDD(dtrain, w) ;
            w = subtr(w,prodbyscal((stepsize/m),s))
        }
        w
    }

    def SGD_RDD(train: RDD[(Double, Array[Double])],stepsize : Double, n : Int, sizefeat : Int): Array[Double] = {
        val m:Double=train.count() // card(train)
        var w = Array.fill(sizefeat)(0.0)
        val traingi = train.glom().zipWithIndex()
        val np = traingi.count().toInt
        for (j <- 0 to np-1)
        {
            // Gradient computation
            //var temp = Array.fill(sizefeat)(0.0)
            //w = traingi.flatMap{case (p,i) => {if (i == j) {Array(LocalGradient2(p,w,stepsize,j,sizefeat))} else Array[Array[Double]]()}}.collect()(0)
            w = traingi.flatMap{ case(p,i) => {if (i == j) {Array(SGD(p,w,stepsize))} else Array[Array[Double]]()}}.collect()(0)
        }
        w
    }

    def SGD_RDD_momentum(train : RDD[(Double, Array[Double])], sizefeat : Int , stepsize : Double, momentum_term : Double) : Array[Double] = {
        var w = Array.fill(sizefeat)(0.0)
        var v = Array.fill(sizefeat)(0.0)
        val traingi = train.glom().zipWithIndex().persist()
        val np = traingi.count().toInt
        for (j <-0 to np-1){
            // calcul du gradient batch après batch
            val res = traingi.flatMap{
                case(p,i) => {if (i == j) {
                    val sgdm = SGD_momentum(p,w,stepsize, v, momentum_term) ;
                    val w_res = sgdm._1 ;
                    val v_res = sgdm._2 ;
                    Array((w_res,v_res)) }
                else Array[(Array[Double],Array[Double])] ()}
            }.collect()(0) ;
            w = res._1 ;
            v = res._2
        }
        w
    }

    // Old tests
    def BaseGradient(train : RDD[(Double, Array[Double])], stepsize : Double, n : Int, sizefeat : Int): Array[Double] = {
        val m:Double=train.count() // card(train)
        var w = Array.fill(sizefeat)(0.0)
        var grad = Array.fill(sizefeat)(0.0)
        // Global loop
        for (j <- 1 to n)
        {
            // Gradient computation
            //LoggerHelper.logWarn(s"ITERATION : ${j}")
            grad = train.map{case (y,x)=> (prodbyscal(2.0*(prods(w,x)-y),x))}.reduce(sum)
            w = subtr(w,prodbyscal((stepsize/m),grad))
        }
        w
    }

    def LocalGradient2(train: Array[(Double, Array[Double])], w: Array[Double],stepsize : Double, n : Int, sizefeat : Int): Array[Double] = {
        //val m:Double=train.count() // card(train)
        var w = Array.fill(sizefeat)(0.0)
        var grad = Array.fill(sizefeat)(0.0)
        // Global loop
        for (couple <- train)
        {
            // Gradient computation
            //LoggerHelper.logWarn(s"ITERATION : ${j}")
            var y = couple._1
            var x = couple._2
            grad = (prodbyscal(2.0*(prods(w,x)-y),x))
            //grad = train.map{case (y,x)=> (prodbyscal(2.0*(prods(w,x)-y),x))}.reduce(sum)
            w = subtr(w,prodbyscal((stepsize/1),grad))
        }
        w
    }

    def LocalGradient(train : Array[(Double, Array[Double])],current : Array[Double],stepsize : Double, sizefeat : Int): Array[Double] = {
        val m:Double=train.length // card(train)
        var w = Array.fill(sizefeat)(0.0)
        var temp = current 
        train.foreach{case (y,x)=> temp = sum(temp,(prodbyscal(2.0*(prods(w,x)-y),x)))
            w = subtr(w,prodbyscal((stepsize/m),temp))
        }
        w
    }

}
