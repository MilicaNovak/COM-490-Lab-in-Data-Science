# Week 9: Homework 3 

This is the final week of the Apache Spark series and consists of a graded 
project notebook found in this repository.

**Due date**: May 7, 18:00

As always, fork this repository into your own namespace. The contents of your 
forked repo on the due date will graded.

## Getting started

The set up is the same as for week 7. We will be using notebooks on the iccluster.

**if you already did this setup for week 7 you can safely skip to the next section!**

The notebooks in this series will most easily be run on the IC Cluster. To set
up your linux `PATH` and `PYTHONPATH` correctly, log in to your iccluster
account and copy/paste the two commands below:

```
echo "export PATH=/opt/anaconda3/bin:$PATH" >> ~/.bash_profile
echo "export PYTHONPATH=/usr/hdp/current/spark2-client/python/lib/py4j-0.10.7-src.zip:/usr/hdp/current/spark2-client/python" >> ~/.bash_profile
echo "export PYSPARK_PYTHON=/opt/anaconda3/bin/python" >> ~/.bash_profile
source ~/.bash_profile
```


## Starting notebooks

To begin this week's exercises, make a fork of this repository in your own
namespace on gitlab and then clone the fork in your account on the iccluster:

```
$ git clone https://git-dslab.epfl.ch/<gitlab-username>/homework3-spark.git
```

Please make a copy of the EMPTY `twitter-hashtags.ipynb` notebook and add your name to the filename, 
for example `twitter-hashtags-calvin.ipynb`. Add 
this notebook to the repo and commit. This way if we need to push an update to 
the EMPTY notebook, you won't get merge conflicts.

In the notebooks, you will see **TODO** sections each worth a number of points.

To start this notebook, run

```
$ jupyter notebook --ip 10.90.38.18
```

Browse to the clone of the repository and open the notebook that you made above
with your name in the filename. 

If you close your browser window on accident and need to get back to your
notebook, you can find the currently-running notebook servers with:

```
$ jupyter notebook list
Currently running servers:
http://10.90.38.21:8889/?token=1234 :: /home/roskar
```

