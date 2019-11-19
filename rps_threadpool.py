# -*- coding: utf-8 -*-

import threading
import Queue


class RpsLimiter(object):
    def __init__(self, start_value):
        self.__value = start_value
        self.__reset_value = start_value
        self.__lock = threading.Lock()
        self.__timer = PerpetualTimer(1.0, self.__reset)
        self.__timer.start()
        self.__available = threading.Event()
        self.__available.set()

    def take(self):
        while True:
            self.__available.wait()
            with self.__lock:
                if self.__value > 0:
                    self.__value -= 1
                    if self.__value == 0:
                        self.__available.clear()
                    return
                else:
                    continue

    def __reset(self):
        with self.__lock:
            self.__value = self.__reset_value
            self.__available.set()

    def stop(self):
        self.__timer.cancel()


class PerpetualTimer(object):
    def __init__(self, interval, callback, *args, **kwargs):
        self.__interval = interval
        self.__callback_function = callback
        self.__timer = threading.Timer(self.__interval, self.__handle_function)
        self.__args = args
        self.__kwargs = kwargs

    def __handle_function(self):
        self.__callback_function(*self.__args, **self.__kwargs)
        self.__timer = threading.Timer(self.__interval, self.__handle_function)
        self.__timer.start()

    def start(self):
        self.__timer.start()

    def cancel(self):
        self.__timer.cancel()


class ClosedPoolFlag(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.value = False


class ClosedPool(Exception):
    pass


def _terminate_worker():
    raise ClosedPool()


class _Worker(threading.Thread):
    def __init__(self, queue, rps_limiter, heartbeat_interval, set_daemon):
        threading.Thread.__init__(self)
        self.__queue = queue
        self.__rps_limiter = rps_limiter
        self.__heartbeat_interval = heartbeat_interval
        self.setDaemon(set_daemon)
        self.start()

    def run(self):
        while True:
            try:
                call_function, args, kwargs = self.__queue.get(timeout=self.__heartbeat_interval)
                self.__rps_limiter.take()
                call_function(*args, **kwargs)
            except Queue.Empty:
                continue
            except ClosedPool:
                self.__queue.task_done()
                break
            self.__queue.task_done()


class QueueThreadPool:
    '''
        Thread pool thread safe class with blocking queue to store input tasks.
        Allows pool to START NOT MORE THAN rps_limit tasks in one second
        >>> with QueueThreadPool(thread_pool_size, queue_maxsize, rps_limit, heartbeat_interval) as q:
        >>>     for i in xrange(0, tasks_sum):
        >>>         q.add_task(do)
    '''

    def __init__(self, thread_pool_size=1, queue_maxsize=0, rps_limit=1, heartbeat_interval=10.0,
                 is_daemon_threadpool=True):
        self.__queue = Queue.Queue(queue_maxsize)
        self.__closed = ClosedPoolFlag()
        self.__rps_limiter = RpsLimiter(rps_limit)
        self.__thread_pool = [_Worker(self.__queue, self.__rps_limiter, heartbeat_interval, is_daemon_threadpool)
                              for i in xrange(0, thread_pool_size)]

    def add_task(self, func, *args, **kwargs):
        with self.__closed.lock:
            if self.__closed.value is True:
                raise ClosedPool()
            self.__queue.put((func, args, kwargs))

    def map(self, func, args_list):
        for args in args_list:
            self.add_task(func, args)

    def join(self):
        with self.__closed.lock:
            self.__closed.value = True
        for i in xrange(0, len(self.__thread_pool)):
            self.__queue.put((_terminate_worker, (), {}))
        for t in self.__thread_pool:
            t.join()
        self.__rps_limiter.stop()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.join()
