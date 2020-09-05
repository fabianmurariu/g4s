# Efectful Graph Library

The aim of this project is to provide generic typeclasses able to abstarct over directed graphs that can fit in memory as well as graphs with edges on GPUs or suprercomputers. This is done by wrapping [GraphBLAS](http://graphblas.org/index.php?title=Graph_BLAS_Forum) with a thin layer of JNI then exposing these calls via cats-effect. This ensures pointers are released correctly when not needed.

For documentation check-out `docs` folder
