#' Parallelization setup for parallelMap.
#'
#' Defines the underlying parallelization mode for [parallelMap()]. Also allows
#' to set a \dQuote{level} of parallelization. Only calls to [parallelMap()]
#' with a matching level are parallelized. The defaults of all settings are
#' taken from your options, which you can also define in your R profile. For an
#' introductory tutorial and information on the options configuration, please go
#' to the project's github page at https://github.com/mlr-org/parallelMap.
#'
#' Currently the following modes are supported, which internally dispatch the
#' mapping operation to functions from different parallelization packages:
#'
#' - **local**: No parallelization with [mapply()]
#' - **multicore**: Multicore execution on a single machine with `parallel::mclapply()`.
#' - **socket**: Socket cluster on one or multiple machines with `parallel::makePSOCKcluster()` and `parallel::clusterMap()`.
#' - **mpi**: Snow MPI cluster on one or multiple machines with [parallel::makeCluster()] and `parallel::clusterMap()`.
#' - **BatchJobs**: Parallelization on batch queuing HPC clusters, e.g., Torque, SLURM, etc., with [BatchJobs::batchMap()].
#'
#' For BatchJobs mode you need to define a storage directory through the
#' argument `storagedir` or the option `parallelMap.default.storagedir`.
#'
#' @param mode (`character(1)`)\cr
#'   Which parallel mode should be used: \dQuote{local}, \dQuote{multicore},
#'   \dQuote{socket}, \dQuote{mpi}, \dQuote{BatchJobs}. Default is the option
#'   `parallelMap.default.mode` or, if not set, \dQuote{local} without parallel
#'   execution.
#' @param cpus (`integer(1)`)\cr
#'   Number of used cpus. For local and BatchJobs mode this argument is ignored.
#'   For socket mode, this is the number of processes spawned on localhost, if
#'   you want processes on multiple machines use `socket.hosts`. Default is the
#'   option `parallelMap.default.cpus` or, if not set, [parallel::detectCores()]
#'   for multicore mode, `max(1, [mpi.universe.size][Rmpi::mpi.universe.size] -
#'   1)` for mpi mode and 1 for socket mode.
#' @param socket.hosts [character]\cr
#'   Only used in socket mode, otherwise ignored. Names of hosts where parallel
#'   processes are spawned. Default is the option
#'   `parallelMap.default.socket.hosts`, if this option exists.
#' @param bj.resources [list]\cr
#'   Resources like walltime for submitting jobs on HPC clusters via BatchJobs.
#'   See [BatchJobs::submitJobs()]. Defaults are taken from your BatchJobs
#'   config file.
#' @param bt.resources [list]\cr
#'   Analog to `bj.resources`.
#'   See [batchtools::submitJobs()].
#' @param logging (`logical(1)`)\cr
#'   Should slave output be logged to files via [sink()] under the `storagedir`?
#'   Files are named `<iteration_number>.log` and put into unique subdirectories
#'   named `parallelMap_log_<nr>` for each subsequent [parallelMap()]
#'   operation. Previous logging directories are removed on `parallelStart` if
#'   `logging` is enabled. Logging is not supported for local mode, because you
#'   will see all output on the master and can also run stuff like [traceback()]
#'   in case of errors. Default is the option `parallelMap.default.logging` or,
#'   if not set, `FALSE`.
#' @param storagedir (`character(1)`)\cr
#'   Existing directory where log files and intermediate objects for BatchJobs
#'   mode are stored. Note that all nodes must have write access to exactly this
#'   path. Default is the current working directory.
#' @param level (`character(1)`)\cr
#'   You can set this so only calls to [parallelMap()] that have exactly the
#'   same level are parallelized. Default is the option
#'   `parallelMap.default.level` or, if not set, `NA` which means all calls to
#'   [parallelMap()] are are potentially parallelized.
#' @param load.balancing (`logical(1)`)\cr
#'   Enables load balancing for multicore, socket and mpi.
#'   Set this to `TRUE` if you have heterogeneous runtimes.
#'   Default is `FALSE`
#' @param show.info (`logical(1)`)\cr
#'   Verbose output on console for all further package calls? Default is the
#'   option `parallelMap.default.show.info` or, if not set, `TRUE`.
#' @param suppress.local.errors (`logical(1)`)\cr
#'   Should reporting of error messages during function evaluations in local
#'   mode be suppressed? Default ist FALSE, i.e. every error message is shown.
#' @param reproducible (`logical(1)`)\cr
#'   Should parallel jobs produce reproducible results when setting a seed?
#'   With this option, `parallelMap()` calls will be reproducible when using
#'   `set.seed()` with the default RNG kind. This is not the case by default
#'   when parallelizing in R, since the default RNG kind "Mersenne-Twister" is
#'   not honored by parallel processes. Instead RNG kind `"L'Ecuyer-CMRG"` needs
#'   to be used to ensure paralllel reproducibility.
#'   Default is the option `parallelMap.default.reproducible` or, if not set,
#'   `TRUE`.
#' @param ... (any)\cr
#'   Optional parameters, for socket mode passed to
#'   `parallel::makePSOCKcluster()`, for mpi mode passed to
#'   [parallel::makeCluster()] and for multicore passed to
#'   `parallel::mcmapply()` (`mc.preschedule` (overwriting `load.balancing`),
#'   `mc.set.seed`, `mc.silent` and `mc.cleanup` are supported for multicore).
#' @return Nothing.
#' @export
parallelStart = function(mode, cpus, socket.hosts, bj.resources = list(),
  bt.resources = list(), logging, storagedir, level, load.balancing = FALSE,
  show.info, suppress.local.errors = FALSE, reproducible, ...) {

  # if stop was not called, warn and do it now

  if (isStatusStarted() && !isModeLocal()) {
    warningf("Parallelization was not stopped, doing it now.")
    parallelStop()
  }

  # FIXME: what should we do onexit if an error happens in this function?

  mode = getPMDefOptMode(mode)
  cpus = getPMDefOptCpus(cpus)
  socket.hosts = getPMDefOptSocketHosts(socket.hosts)
  reproducible = getPMDefOptReproducible(reproducible)

  level = getPMDefOptLevel(level)
  rlevls = parallelGetRegisteredLevels(flatten = TRUE)
  if (!is.na(level) && level %nin% rlevls) {
    warningf(
      "Selected level='%s' not registered! This is likely an error! Note that you can also
      register custom levels yourself to get rid of this warning, see ?parallelRegisterLevels.R",
      level)
  }
  logging = getPMDefOptLogging(logging)
  storagedir = getPMDefOptStorageDir(storagedir)
  # defaults are in batchjobs conf
  assertList(bj.resources)
  assertList(bt.resources)
  assertFlag(load.balancing)
  show.info = getPMDefOptShowInfo(show.info)

  # multicore not supported on windows
  if (mode == MODE_MULTICORE && .Platform$OS.type == "windows") {
    stop("Multicore mode not supported on windows!")
  }
  assertDirectoryExists(storagedir, access = "w")

  # store options for session, we already need them for helper funs below
  options(parallelMap.mode = mode)
  options(parallelMap.level = level)
  options(parallelMap.logging = logging)
  options(parallelMap.storagedir = storagedir)
  options(parallelMap.bj.resources = bj.resources)
  options(parallelMap.bt.resources = bt.resources)
  options(parallelMap.load.balancing = load.balancing)
  options(parallelMap.show.info = show.info)
  options(parallelMap.status = STATUS_STARTED)
  options(parallelMap.nextmap = 1L)
  options(parallelMap.suppress.local.errors = suppress.local.errors)
  options(parallelMap.reproducible = reproducible)

  # try to autodetect cpus if not set
  if (is.na(cpus) && mode %in% c(MODE_MULTICORE, MODE_MPI)) {
    cpus = autodetectCpus(mode)
  }
  if (isModeSocket()) {
    if (!is.na(cpus) && !is.null(socket.hosts)) {
      stopf("You cannot set both cpus and socket.hosts in socket mode!")
    }
    if (is.na(cpus) && is.null(socket.hosts)) {
      cpus = 1L
    }
  }
  if (isModeLocal()) {
    if (!is.na(cpus)) {
      stopf("Setting %i cpus makes no sense for local mode!", cpus)
    }
  }

  options(parallelMap.cpus = cpus)

  showStartupMsg(mode, cpus, socket.hosts)

  # now load extra packs we need
  requirePackages(getExtraPackages(mode), why = "parallelStart")

  # delete log dirs from previous runs
  if (logging) {
    if (isModeLocal()) {
      stop("Logging not supported for local mode!")
    }
    deleteAllLogDirs()
  }

  # init parallel packs / modes, if necessary
  if (isModeMulticore()) {
    args = list(...)
    args$mc.preschedule = args$mc.preschedule %??% !load.balancing



    cl = do.call(makeMulticoreCluster, args)

  } else if (isModeSocket()) {
    # set names from cpus or socket.hosts, only 1 can be defined here
    if (is.na(cpus)) {
      names = socket.hosts
    } else {
      names = cpus
    }
    cl = makePSOCKcluster(names = names, ...)
    if (reproducible) {
      clusterSetRNGStream(cl, iseed = sample(1:100000, 1))
    }
    setDefaultCluster(cl)
  } else if (isModeMPI()) {
    cl = makeCluster(spec = cpus, type = "MPI", ...)
    if (reproducible) {
      clusterSetRNGStream(cl, iseed = sample(1:100000, 1))
    }
    setDefaultCluster(cl)
  } else if (isModeBatchJobs()) {
    # create registry in selected directory with random, unique name
    fd = getBatchJobsNewRegFileDir()
    suppressMessages({
      BatchJobs::makeRegistry(id = basename(fd), file.dir = fd, work.dir = getwd())
    })
  } else if (isModeBatchtools()) {
    fd = getBatchtoolsNewRegFileDir()
    old = getOption("batchtools.verbose")
    options(batchtools.verbose = FALSE)
    on.exit(options(batchtools.verbose = old))
    reg = batchtools::makeRegistry(file.dir = fd, work.dir = getwd())
  }
  invisible(NULL)
}

#' @export
#' @rdname parallelStart
parallelStartLocal = function(show.info, suppress.local.errors = FALSE, ...) {
  parallelStart(
    mode = MODE_LOCAL, cpus = NA_integer_, level = NA_character_,
    logging = FALSE, show.info = show.info,
    suppress.local.errors = suppress.local.errors, ...)
}

#' @export
#' @rdname parallelStart
parallelStartMulticore = function(cpus, logging, storagedir, level,
  load.balancing = FALSE, show.info, reproducible, ...) {
  parallelStart(
    mode = MODE_MULTICORE, cpus = cpus, level = level,
    logging = logging, storagedir = storagedir, load.balancing = load.balancing,
    show.info = show.info, reproducible = reproducible, ...)
}

#' @export
#' @rdname parallelStart
parallelStartSocket = function(cpus, socket.hosts, logging, storagedir, level,
  load.balancing = FALSE, show.info, reproducible, ...) {
  parallelStart(
    mode = MODE_SOCKET, cpus = cpus, socket.hosts = socket.hosts,
    level = level, logging = logging, storagedir = storagedir,
    load.balancing = load.balancing, show.info = show.info,
    reproducible = reproducible, ...)
}

#' @export
#' @rdname parallelStart
parallelStartMPI = function(cpus, logging, storagedir, level,
  load.balancing = FALSE, show.info, reproducible, ...) {
  parallelStart(
    mode = MODE_MPI, cpus = cpus, level = level, logging = logging,
    storagedir = storagedir, load.balancing = load.balancing,
    show.info = show.info, reproducible = reproducible, ...)
}

#' @export
#' @rdname parallelStart
parallelStartBatchJobs = function(bj.resources = list(), logging, storagedir, level, show.info, ...) {
  parallelStart(
    mode = MODE_BATCHJOBS, level = level, logging = logging,
    storagedir = storagedir, bj.resources = bj.resources, show.info = show.info, ...)
}

#' @export
#' @rdname parallelStart
parallelStartBatchtools = function(bt.resources = list(), logging, storagedir, level, show.info, ...) {
  parallelStart(
    mode = MODE_BATCHTOOLS, level = level, logging = logging,
    storagedir = storagedir, bt.resources = bt.resources, show.info = show.info, ...)
}
