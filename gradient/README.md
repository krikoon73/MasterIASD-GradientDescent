# Gradient descent educative coding

`./src/main/scala/org/krikoon73/gradient/GradientDescent.scala` : Main program

`./src/main/scala/org/krikoon73/lib/packages.scala` : Algo. + maths lib

`./src/main/scala/org/krikoon73/lib/tools.scala` : miscallenous functions 

`./lib/scopt_2.12-4.0.0.jar` : jar for scopt library

Command example : 

`$SPARK_HOME/bin/spark-submit --jars lib/scopt_2.12-4.0.0.jar target/scala-2.12/gradientdescent_2.12-1.0.jar -a TEST -m "local[*]" -s 0.00000000001 -i 10 -f 2`

