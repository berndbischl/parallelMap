# parallelMap

R package to interface some popular parallelization back-ends with a unified interface.

[![Travis build status](https://img.shields.io/travis/mlr-org/parallelMap/master?logo=travis&style=flat-square&label=Linux)](https://travis-ci.org/mlr-org/parallelMap)
[![AppVeyor build status](https://img.shields.io/appveyor/ci/mlr-org/parallelMap?label=Windows&logo=appveyor&style=flat-square)](https://ci.appveyor.com/project/mlr-org/parallelMap)
[![CRAN Status Badge](http://www.r-pkg.org/badges/version/parallelMap)](http://cran.r-project.org/web/packages/parallelMap)
[![Codecov test coverage](https://codecov.io/gh/mlr-org/parallelMap/branch/master/graph/badge.svg)](https://codecov.io/gh/mlr-org/parallelMap?branch=master)
[![CRAN Downloads](http://cranlogs.r-pkg.org/badges/parallelMap)](http://cran.rstudio.com/web/packages/parallelMap/index.html)

* Offical CRAN release site:
  http://cran.r-project.org/web/packages/parallelMap/index.html

* R Documentation in HTML:
  http://www.rdocumentation.org/packages/parallelMap/

* Run this in R to install the current GitHub version:
  ```r
  remotes::install_github("mlr-org/parallelMap")
  ```

* [Further installation instructions](https://github.com/tudo-r/PackagesInfo/wiki/Installation-Information)

# Overview

parallelMap was written with users (like me) in mind who want a unified parallelization procedure in R that

* Works equally well in interactive operations as in developing packages where some operations should offer the possibility to be run in parallel by the client user of your package.
* Allows the client user of your developed package to completely configure the parallelization from the outside.
* Allows you to be lazy and forgetful. This entails: The same interface for every back-end and everything is easily configurable via options.
* Supports the most important parallelization modes. For me, these currently are: usage of multiple cores on a single machine, socket mode (because it also works on Windows), MPI and HPC clusters (the latter interfaced by our BatchJobs package).
* Does not make debugging annoying and tedious.

# Mini Tutorial

Here is a short tutorial that already contains the most important concepts and operations:

```splus
##### Example 1) #####

library(parallelMap)
parallelStartSocket(2)    # start in socket mode and create 2 processes on localhost
f = function(i) i + 5     # define our job
y = parallelMap(f, 1:2)   # like R's Map but in parallel
parallelStop()            # turn parallelization off again
```

If you want to use other modes of parallelization, simply call the appropriate initialization procedure, all of them are documented in [parallelStart](http://www.rdocumentation.org/packages/parallelMap/functions/parallelStart.html). [parallelStart](http://www.rdocumentation.org/packages/parallelMap/functions/parallelStart.html) is a catch-all procedure, that allows to set all possible options of the package, but for every mode a variant of [parallelStart](http://www.rdocumentation.org/packages/parallelMap/functions/parallelStart.html) exists with a smaller, appropriate interface.

# Exporting to Slaves: Libraries, Sources and Objects

In many (more complex) applications you somehow need to initialize the slave processes, especially for MPI, socket and BatchJobs mode, where fresh R processes are started. This means: loading of packages, sourcing files with function and object definitions and exporting R objects to the global environment of the slaves.

parallelMap supports these operations with the following three functions

 * [parallelLibrary](http://www.rdocumentation.org/packages/parallelMap/functions/parallelLibrary.html)
 * [parallelSource](http://www.rdocumentation.org/packages/parallelMap/functions/parallelSource.html)
 * [parallelExport](http://www.rdocumentation.org/packages/parallelMap/functions/parallelExport.html)

Let's start with loading a package on the slaves. Of course you could put a require("mypackage") into the body of f, but you can also use a [parallelLibrary](http://www.rdocumentation.org/packages/parallelMap/functions/parallelLibrary.html) before calling [parallelMap](http://www.rdocumentation.org/packages/parallelMap/functions/parallelMap.html).

```r
##### Example 2) #####

library(parallelMap)
parallelStartSocket(2)
parallelLibrary("MASS")
# subsample iris, fit an LDA model and return prediction error
f = function(i) {
  n = nrow(iris)
  train = sample(n, n/2)
  test = setdiff(1:n, train)
  model = lda(Species~., data=iris[train,])
  pred = predict(model, newdata=iris[test,])
  mean(pred$class != iris[test,]$Species)
}
y = parallelMap(f, 1:2)
parallelStop()
```

And here is a further example where we export a big matrix to the slaves, then
apply a preprocessing function to it, which is defined in source file. Yeah, it is kinda
a nonsensical example but I suppose you will get the point:

```r
##### Example 3) #####

library(parallelMap)
parallelStartSocket(2)
parallelSource("preproc.R") # contains definition of preproc()
bigmatrix = matrix(1, nrow=500, ncol=500)
parallelExport("bigmatrix")
f = function(i) {
  p = preproc(bigmatrix)
  p + i
}
y = parallelMap(f, 1:2)
parallelStop()
```

# Being Lazy: Configuration

On a given system, you will probably always parallelize you operations in a similar fashion. For this reason, [parallelMap](http://www.rdocumentation.org/packages/parallelMap/functions/parallelMap.html) allows you to define defaults for all relevant settings through R's option mechanism in , e.g., your R profile.

Let's assume on your office PC you run some Unix-like operating system and have 4 cores at your disposal. You are also an experienced user and don't need [parallelMap](http://www.rdocumentation.org/packages/parallelMap/functions/parallelMap.html)'s "chatting" on the console anymore. Simply define these lines in your R profile:


```r
options(
  parallelMap.default.mode        = "multicore",
  parallelMap.default.cpus        = 4,
  parallelMap.default.show.info   = FALSE
)
```

This allows you to save some typing as running [parallelStart()](http://www.rdocumentation.org/packages/parallelMap/functions/parallelStart.html) will now be equivalent to parallelStart(mode = "multicore", cpus=4, show.info=FALSE) so "Example 1" would become:

```r
parallelStart()
f = function(i) i + 5
y = parallelMap(f, 1:2)
parallelStop()
```

You can later always overwrite settings be explicitly passing them to [parallelStart](http://www.rdocumentation.org/packages/parallelMap/functions/parallelStart.html), so


```r
parallelStart(cpus=2)
f = function(i) i + 5
y = parallelMap(f, 1:2)
parallelStop()
```

would use your default "multicore" mode and still disable [parallelMap](http://www.rdocumentation.org/packages/parallelMap/functions/parallelMap.html)'s info messages on the console, but decrease cpu usage to 2.

The following options are currently available:

```r
  parallelMap.default.mode            = "local" / "multicore" / "socket" / "mpi" / "BatchJobs"
  parallelMap.default.cpus            = <integer>
  parallelMap.default.level           = <string> or NA
  parallelMap.default.socket.hosts    = character vector of host names where to spawn in socket mode
  parallelMap.default.show.info       = TRUE / FALSE
  parallelMap.default.logging         = TRUE / FALSE
  parallelMap.default.storagedir      = <path>, must be on a shared file system for master / slaves
```

For their precise meaning please read the documentation of [parallelStart](http://www.rdocumentation.org/packages/parallelMap/functions/parallelStart.html).

# Package development: Tagging mapping operations with a level name

Sometimes it is useful to have more control over which `parallelMap` operation is actually parallelized.
You can tag parallelMap operations with a so-called "level", basically a name
or category associated with the operation. Usually you would do this in a client package, but you can also do it in custom code.
For packages, register the level(s) that you define in `zzz.R` to tell parallelMap
about them.
Here is an example from mlr's
[zzz.R](https://github.com/berndbischl/mlr/blob/master/R/zzz.R)
where we call this in .onAttach

```r
.onAttach = function(libname, pkgname) {
  # ...
  parallelRegisterLevels(package = "mlr", levels = c("benchmark", "resample", "selectFeatures", "tuneParams"))
}
```

Later on the user can ask what levels are available, for example

```r
library(mlr)
parallelGetRegisteredLevels()
> mlr: mlr.benchmark, mlr.resample, mlr.selectFeatures, mlr.tuneParams
```

The output shows the registered levels for each package; in this example, only
one package is loaded that provides levels.

In the client package, the tagging of the `parallelMap` operation is done through
the `level` argument:

```r
parallelMap(myfun, 1:n, level = "package.levelname")
```

In mlr, we tag parallel operations with such a level, e.g.,
[here](https://github.com/berndbischl/mlr/blob/master/R/resample.R).

The user of the package can now set the level when starting the parallel backend, again through the `level` argument:

```r
parallelStartSocket(ncpus = 2L, level = "package.levelname")
```

Parallelization is now performed as follows:

* If no level is set in `parallelStart`, the first encountered `parallelMap` call on the master is parallelized, whether it has a tag or not.
* If a level is set in the call to `parallelStart`, only the `parallelMap` calls which have exactly this level set and run on the master are parallelised.
* No further parallelization is done if we are already on a slave, i.e. if the
  parent call has already been parallelised through `parallelMap`.

Please read the documentation of

 * [parallelRegisterLevels](http://www.rdocumentation.org/packages/parallelMap/functions/parallelRegisterLevels)
 * [parallelStart](http://www.rdocumentation.org/packages/parallelMap/functions/parallelStart)
 * [parallelMap](http://www.rdocumentation.org/packages/parallelMap/functions/parallelMap)

for more detailed information regarding this topic.
