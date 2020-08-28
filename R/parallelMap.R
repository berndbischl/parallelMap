#' @title Maps a function over lists or vectors in parallel.
#'
#' @description
#' Uses the parallelization mode and the other options specified in
#' [parallelStart()].
#'
#' Libraries and source file can be initialized on slaves with
#' [parallelLibrary()] and [parallelSource()].
#'
#' Large objects can be separately exported via [parallelExport()],
#' they can be simply used under their exported name in slave body code.
#'
#' Regarding error handling, see the argument `impute.error`.
#'
#' @param fun [function]\cr
#'   Function to map over `...`.
#' @param ... (any)\cr
#'   Arguments to vectorize over (list or vector).
#' @param more.args [list]\cr
#'   A list of other arguments passed to `fun`.
#'   Default is empty list.
#' @param simplify (`logical(1)`)\cr
#'   Should the result be simplified? See [simplify2array]. If `TRUE`,
#'   `simplify2array(higher = TRUE)` will be called on the result object.
#'   Default is `FALSE`.
#' @param use.names (`logical(1)`)\cr
#'   Should result be named?
#'   Use names if the first `...` argument has names, or if it is a
#'   character vector, use that character vector as the names.
#' @param impute.error (`NULL` | `function(x)`)\cr
#'   This argument can be used for improved error handling. `NULL` means that,
#'   if an exception is generated on one of the slaves, it is also thrown on the
#'   master. Usually all slave jobs will have to terminate until this exception
#'   on the master can be thrown. If you pass a constant value or a function,
#'   all jobs are guaranteed to return a result object, without generating an
#'   exception on the master for slave errors. In case of an error, this is a
#'   [simpleError()] object containing the error message. If you passed a
#'   constant object, the error-objects will be substituted with this object. If
#'   you passed a function, it will be used to operate on these error-objects
#'   (it will ONLY be applied to the error results). For example, using
#'   `identity` would  keep and return the `simpleError`-object, or `function(x)
#'   99` would impute a constant value (which could be achieved more easily by
#'   simply passing `99`). Default is `NULL`.
#' @param level (`character(1)`)\cr
#'   If a (non-missing) level is specified in [parallelStart()],
#'   this call is only parallelized if the level specified here matches.
#'   Useful if this function is used in a package.
#'   Default is `NA`.
#' @param show.info (`logical(1)`)\cr
#'   Verbose output on console?
#'   Can be used to override setting from options / [parallelStart()].
#'   Default is NA which means no overriding.
#' @return Result.
#' @export
#' @examples
#' parallelStart()
#' parallelMap(identity, 1:2)
#' parallelStop()
parallelMap = function(fun, ..., more.args = list(), simplify = FALSE,
  use.names = FALSE, impute.error = NULL, level = NA_character_,
  show.info = NA) {

  assertFunction(fun)
  assertList(more.args)
  assertFlag(simplify)
  assertFlag(use.names)
  # if it is a constant value construct function to impute
  if (!is.null(impute.error)) {
    if (is.function(impute.error)) {
      impute.error.fun = impute.error
    } else {
      impute.error.fun = function(x) impute.error
    }
  }
  assertString(level, na.ok = TRUE)
  assertFlag(show.info, na.ok = TRUE)

  if (!is.na(level) && level %nin% unlist(getPMOption("registered.levels", list()))) {
    stopf("Level '%s' not registered", level)
  }

  cpus = getPMOptCpus()
  load.balancing = getPMOptLoadBalancing()
  logging = getPMOptLogging()
  reproducible = getPMOptReproducible()
  # use NA to encode "no logging" in logdir
  logdir = ifelse(logging, getNextLogDir(), NA_character_)

  if (isModeLocal() || !isParallelizationLevel(level) || getPMOptOnSlave()) {
    if (!is.null(impute.error)) {
      # so we behave in local mode as in parallelSlaveWrapper
      fun2 = function(...) {
        res = try(fun(...), silent = getOption("parallelMap.suppress.local.errors"))
        if (BBmisc::is.error(res)) {
          res = list(try.object = res)
          class(res) = "parallelMapErrorWrapper"
        }
        return(res)
      }
    } else {
      fun2 = fun
    }
    assignInFunctionNamespace(fun, env = PKG_LOCAL_ENV)
    res = mapply(fun2, ..., MoreArgs = more.args, SIMPLIFY = FALSE, USE.NAMES = FALSE)
  } else {
    iters = seq_along(..1)
    showInfoMessage("Mapping in parallel%s: mode = %s; level = %s; cpus = %i; elements = %i.",
      ifelse(load.balancing, " (load balanced)", ""), getPMOptMode(),
      level, getPMOptCpus(), length(iters), show.info = show.info)

    if (isModeMulticore()) {
      more.args = c(list(.fun = fun, .logdir = logdir), more.args)
      if (reproducible) {
        old.seed = .Random.seed
        old.rng.kind = RNGkind()
        seed = sample(1:100000, 1)
        # we need to reset the seed first in case the user supplied a seed,
        # otherwise "L'Ecuyer-CMRG" won't be used
        rm(.Random.seed, envir = globalenv())
        set.seed(seed, "L'Ecuyer-CMRG")
      }
      res = MulticoreClusterMap(slaveWrapper, ..., .i = iters,
        MoreArgs = more.args, mc.cores = cpus,
        SIMPLIFY = FALSE, USE.NAMES = FALSE)
      if (reproducible) {
        # restore initial RNGkind
        .Random.seed = old.seed
        RNGkind(old.rng.kind[1], old.rng.kind[2], old.rng.kind[3])
      }
    } else if (isModeSocket() || isModeMPI()) {
      more.args = c(list(.fun = fun, .logdir = logdir), more.args)
      if (load.balancing) {
        res = clusterMapLB(cl = NULL, slaveWrapper, ..., .i = iters,
          MoreArgs = more.args)
      } else {
        res = clusterMap(cl = NULL, slaveWrapper, ..., .i = iters,
          MoreArgs = more.args, SIMPLIFY = FALSE, USE.NAMES = FALSE)
      }
    } else if (isModeBatchJobs()) {
      # dont log extra in BatchJobs
      more.args = c(list(.fun = fun, .logdir = NA_character_), more.args)
      suppressMessages({
        reg = getBatchJobsReg()
        # FIXME: this should be exported by BatchJobs ...
        asNamespace("BatchJobs")$dbRemoveJobs(reg, BatchJobs::getJobIds(reg))
        BatchJobs::batchMap(reg, slaveWrapper, ..., more.args = more.args)
        # increase max.retries a bit, we dont want to abort here prematurely
        # if no resources set we submit with the default ones from the bj conf
        BatchJobs::submitJobs(reg, resources = getPMOptBatchJobsResources(), max.retries = 15)
        ok = BatchJobs::waitForJobs(reg, stop.on.error = is.null(impute.error))
      })
      # copy log files of terminated jobs to designated dir
      if (!is.na(logdir)) {
        term = BatchJobs::findTerminated(reg)
        fns = BatchJobs::getLogFiles(reg, term)
        dests = file.path(logdir, sprintf("%05i.log", term))
        file.copy(from = fns, to = dests)
      }
      ids = BatchJobs::getJobIds(reg)
      ids.err = BatchJobs::findErrors(reg)
      ids.exp = BatchJobs::findExpired(reg)
      ids.done = BatchJobs::findDone(reg)
      ids.notdone = c(ids.err, ids.exp)
      # construct notdone error messages
      msgs = rep("Job expired!", length(ids.notdone))
      msgs[ids.err] = BatchJobs::getErrorMessages(reg, ids.err)
      # handle errors (no impute): kill other jobs + stop on master
      if (is.null(impute.error) && length(c(ids.notdone)) > 0) {
        extra.msg = sprintf("Please note that remaining jobs were killed when 1st error occurred to save cluster time.\nIf you want to further debug errors, your BatchJobs registry is here:\n%s",
          reg$file.dir)
        onsys = BatchJobs::findOnSystem(reg)
        suppressMessages(
          BatchJobs::killJobs(reg, onsys)
        )
        onsys = BatchJobs::findOnSystem(reg)
        if (length(onsys) > 0L) {
          warningf("Still %i jobs from operation on system! kill them manually!", length(onsys))
        }
        if (length(ids.notdone) > 0L) {
          stopWithJobErrorMessages(ids.notdone, msgs, extra.msg)
        }
      }
      # if we reached this line and error occurred, we have impute.error != NULL (NULL --> stop before)
      res = vector("list", length(ids))
      res[ids.done] = BatchJobs::loadResults(reg, simplify = FALSE, use.names = FALSE)
      res[ids.notdone] = lapply(msgs, function(s) impute.error.fun(simpleError(s)))
    } else if (isModeBatchtools()) {
      # don't log extra in batchtools
      more.args = insert(more.args, list(.fun = fun, .logdir = NA_character_))

      old = getOption("batchtools.verbose")
      options(batchtools.verbose = FALSE)
      on.exit(options(batchtools.verbose = old))

      reg = getBatchtoolsReg()
      if (nrow(reg$status) > 0L) {
        batchtools::clearRegistry(reg = reg)
      }
      ids = batchtools::batchMap(fun = slaveWrapper, ..., more.args = more.args, reg = reg)
      batchtools::submitJobs(ids = ids, resources = getPMOptBatchtoolsResources(), reg = reg)
      ok = batchtools::waitForJobs(ids = ids, stop.on.error = is.null(impute.error), reg = reg)

      # copy log files of terminated jobs to designated directory
      if (!is.na(logdir)) {
        x = batchtools::findStarted(reg = reg)
        x$log.file = file.path(reg$file.dir, "logs", sprintf("%s.log", x$job.hash))
        .mapply(function(id, fn) writeLines(batchtools::getLog(id, reg = reg), con = fn), x, NULL)
      }

      if (ok) {
        res = batchtools::reduceResultsList(ids, reg = reg)
      } else {
        if (is.null(impute.error)) {
          extra.msg = sprintf("Please note that remaining jobs were killed when 1st error occurred to save cluster time.\nIf you want to further debug errors, your batchtools registry is here:\n%s",
            reg$file.dir)
          batchtools::killJobs(reg = reg)
          ids.notdone = batchtools::findNotDone(reg = reg)
          stopWithJobErrorMessages(
            inds = ids.notdone$job.id,
            batchtools::getErrorMessages(ids.notdone, missing.as.error = TRUE, reg = reg)$message,
            extra.msg)
        } else { # if we reached this line and error occurred, we have impute.error != NULL (NULL --> stop before)
          res = batchtools::findJobs(reg = reg)
          res$result = list(NULL)
          ids.complete = batchtools::findDone(reg = reg)
          ids.incomplete = batchtools::findNotDone(reg = reg)
          res[match(ids.complete$job.id, res$job.id), "result"] = list(batchtools::reduceResultsList(ids.complete, reg = reg))
          res[match(ids.incomplete$job.id, res$job.id), "result"] = list(lapply(batchtools::getErrorMessages(ids.incomplete, reg = reg)$message, simpleError))
          res = res$result
        }
      }
    }
  }

  # handle potential errors in res, depending on user setting
  if (is.null(impute.error)) {
    checkResultsAndStopWithErrorsMessages(res)
  } else {
    res = lapply(res, function(x) {
      if (inherits(x, "parallelMapErrorWrapper")) {
        impute.error.fun(attr(x$try.object, "condition"))
      } else {
        x
      }
    })
  }

  if (use.names && !is.null(names(..1))) {
    names(res) = names(..1)
  } else if (use.names && is.character(..1)) {
    names(res) = ..1
  } else if (!use.names) {
    names(res) = NULL
  }
  if (isTRUE(simplify) && length(res) > 0L) {
    res = simplify2array(res, higher = simplify)
  }

  # count number of mapping operations for log dir
  options(parallelMap.nextmap = (getPMOptNextMap() + 1L))

  return(res)
}

slaveWrapper = function(..., .i, .fun, .logdir = NA_character_) {

  if (!is.na(.logdir)) {
    options(warning.length = 8170L, warn = 1L)
    .fn = file.path(.logdir, sprintf("%05i.log", .i))
    .fn = file(.fn, open = "wt")
    .start.time = as.integer(Sys.time())
    sink(.fn)
    sink(.fn, type = "message")
    on.exit(sink(NULL))
  }

  # make sure we dont parallelize any further
  options(parallelMap.on.slave = TRUE)
  # just make sure, we should not have changed anything on the master
  # except for BatchJobs / interactive
  on.exit(options(parallelMap.on.slave = FALSE))

  # wrap in try block so we can handle error on master
  res = try(.fun(...))
  # now we cant simply return the error object, because clusterMap would act on it. great...
  if (BBmisc::is.error(res)) {
    res = list(try.object = res)
    class(res) = "parallelMapErrorWrapper"
  }
  if (!is.na(.logdir)) {
    .end.time = as.integer(Sys.time())
    print(gc())
    message(sprintf("Job time in seconds: %i", .end.time - .start.time))
    # I am not sure why i need to do this again, but without i crash in multicore
    sink(NULL)
  }
  return(res)
}

assignInFunctionNamespace = function(fun, li = list(), env = new.env()) {
  # copy exported objects in PKG_LOCAL_ENV to env of fun so we can find them in any case in call
  ee = environment(fun)
  ns = ls(env)
  for (n in ns) {
    assign(n, get(n, envir = env), envir = ee)
  }
  ns = names(li)
  for (n in ns) {
    assign(n, li[[n]], envir = ee)
  }
}
