package com.reconciliation.framework.util

import org.apache.log4j.Logger

trait Logging {
  @transient private var log_ : Logger = null

  protected def log: Logger = {
    if (log_ == null) {
      log_ = Logger.getLogger(getClass.getName)
    }
    log_
  }
}
