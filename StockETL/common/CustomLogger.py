import sys
import time
import inspect
import logging
import functools

__doc__ = """Custom logger to replace traditional Log"""


def get_logger(name, level=logging.DEBUG):
    """Get Logger"""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if logger.handlers:
        # or else, as I found out, we keep adding handlers and duplicate messages
        pass
    else:
        ch = logging.StreamHandler(sys.stderr)
        ch.setLevel(level)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(level)s - %(message)s"
        )
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    return logger


def with_debug_logging(_func=None, *, logger_module=None):
    """Decorator to enable logging tracking"""

    def decorator_log(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            logger = logger_module if logger_module else get_logger("StockETL")
            args_repr = [repr(a) for a in args]
            kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
            signature = ", ".join(args_repr + kwargs_repr)
            start_info_msg = f"START {func.__qualname__}"
            start_debug_msg = f"START {func.__qualname__} called with args {signature}"
            logger.info(start_info_msg)
            logger.debug(start_debug_msg)
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                msg = f"FINISH {func.__qualname__} - {duration:.2f} seconds"
                logger.info(msg)
                return result
            except Exception as e:
                exception_msg = f"**ERROR -- {func.__qualname__}. exception: {str(e)} - params: {signature}"
                logger.exception(exception_msg)
                raise e

        return wrapper

    if _func is None:
        return decorator_log
    else:
        return decorator_log(_func)


def with_processor_logging(_func=None, *, logger_module=None):
    """Decorator to enable logging tracking"""

    def decorator_log(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            logger = logger_module if logger_module else get_logger("StockETL")
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                msg = f"FINISH {func.__name__} - Result count: {result.count():,}- {duration:.2f} seconds"
                logger.info(msg)
                return result
            except Exception as e:
                exception_msg = f"**ERROR -- {func.__qualname__}. exception: {str(e)}"
                logger.exception(exception_msg)
                raise e

        return wrapper

    if _func is None:
        return decorator_log
    else:
        return decorator_log(_func)


def with_logging(_func=None, *, logger_module=None):
    """Decorator to enable logging tracking"""

    def decorator_log(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            logger = logger_module if logger_module else get_logger("StockETL")
            args_repr = [repr(a) for a in args]
            kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
            signature = ", ".join(args_repr + kwargs_repr)
            start_msg = f"START {func.__qualname__}"
            logger.info(start_msg)
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                msg = f"FINISH {func.__qualname__} - {duration:.2f} seconds"
                logger.info(msg)
                return result
            except Exception as e:
                exception_msg = f"**ERROR -- {func.__qualname__}. exception: {str(e)} - Params called: {signature}"
                logger.exception(exception_msg)
                raise e

        return wrapper

    if _func is None:
        return decorator_log
    else:
        return decorator_log(_func)


def decorator_for_func(orig_func):
    def decorator(*args, **kwargs):
        print("Decorating wrapper called for method %s" % orig_func.__name__)
        result = orig_func(*args, **kwargs)
        return result

    return decorator


def with_logging_whole_class(cls):
    """Decorator to enable whole class"""
    for name, method in inspect.getmembers(cls):
        if (
            not inspect.ismethod(method) and not inspect.isfunction(method)
        ) or inspect.isbuiltin(method):
            continue
        setattr(cls, name, decorator_for_func(method))
    return cls
