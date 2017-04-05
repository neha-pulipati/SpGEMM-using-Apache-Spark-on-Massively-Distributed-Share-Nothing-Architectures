import os, sys
from scipy.io.mmio import mminfo, mmread, mmwrite, MMFile
import scipy.sparse
import numpy
#from os import listdir
import timeit,math

os.environ['SPARK_HOME'] = "/Users/nehapulipati/Downloads/spark-1.6.0"
sys.path.append("/Users/nehapulipati/Downloads/spark-1.6.0/python")
sys.path.append("/Users/nehapulipati/Downloads/spark-1.6.0/python/lib")

try:

    from pyspark import SparkContext , SparkConf
    from pyspark.mllib.linalg import Matrices
    from pyspark.mllib.linalg.distributed import BlockMatrix
    from pyspark.sql import SQLContext
    conf = SparkConf().setAppName("SparseMultiplication").setMaster("local[4]")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    print("Successfully imported Spark")

except ImportError as e:
    print("Can not import Spark", e)
    sys.exit(1)

"""if __name__ == "__main__":
    def myFunc(s):
        words = s.split("\n")
        return len(words)"""


fname = '/Users/nehapulipati/Downloads/bcsstk01.mtx'
mat = mmread(fname)
sp_rows, sp_cols, nnzs = mminfo(fname)[0:3]
newmat = mat.tocsc()
sm3 = scipy.sparse.rand(3,2,density=0.2,format='csc')
rand_mat = scipy.sparse.rand(sp_cols, sp_cols, density=0.2,format='csc')
value1 = newmat.data
col_index1 = newmat.indices
row_pointers1 = newmat.indptr

value2 = rand_mat.data
col_index2 = rand_mat.indices
row_pointers2 = rand_mat.indptr

start2 = timeit.default_timer()
just = newmat * rand_mat
stop2 = timeit.default_timer()
t2 = (stop2 - start2)
print t2


sparse1 = Matrices.sparse(sp_rows, sp_cols, row_pointers1, col_index1, value1)
sparse2 = Matrices.sparse(sp_cols, sp_cols, row_pointers2, col_index2, value2)

"""sparse1 = newmat.toarray()
sparse2 = rand_mat.toarray()
print sparse2"""



r1 = sp_rows/2
c1 = sp_cols/2
r2 = sp_cols/2
c2 = sp_cols/2

"""a, b, c, d = sparse1[:r1, :c1], sparse1[r1:, :c1], sparse1[:r1, c1:], sparse1[r1:, c1:]
e, f, g, h = sparse2[:r2, :c2], sparse2[r2:, :c2], sparse2[:r2, c2:], sparse2[r2:, c2:]

blocks1 = sc.parallelize([((0, 0), a),((1,0), b), ((0,1),c), ((1,1),d)])
blocks2 = sc.parallelize([((0, 0), e),((0,1), f), ((0,1),g), ((1,1),h)])"""

blocks1 = sc.parallelize([((0, 0), sparse1), ((0, 1), sparse1)])
blocks2 = sc.parallelize([((0, 0), sparse2), ((1, 0), sparse2)])

num = blocks1.getNumPartitions()
print num

"""mat1 = BlockMatrix(blocks1, 2, 2, 2, 2)
mat2 = BlockMatrix(blocks2, 2, 2, 2, 2)

start1 = timeit.default_timer()
res1 = mat1.multiply(mat2)
stop1 = timeit.default_timer()
t1 = (stop1 - start1)
print t1"""

