
from Queue import Queue
from threading import Thread

class Worker(Thread):
    """Thread executing tasks from a given tasks queue"""
    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            try:
                func, args, kargs = self.tasks.get()
                try:
                    func(*args, **kargs)
                except Exception, e:
                    print "ThreadPool Worker exception: %s" % str(e)
                except:
                    print "ThreadPool Worker unknow exception."
            except:
                print "ThreadPool Worker unknow exception1"
            finally:
                self.tasks.task_done()

class ThreadPool:
    """Pool of threads consuming tasks from a queue"""
    def __init__(self, num_threads):
        self.tasks = Queue()
        for _ in range(num_threads): Worker(self.tasks)

    def add_task(self, func, *args, **kargs):
        """Add a task to the queue"""
        self.tasks.put((func, args, kargs))

    def wait_completion(self):
        """Wait for completion of all the tasks in the queue"""
        self.tasks.join()

    def is_empty(self):
        return self.tasks.empty()

if __name__ == '__main__':
    from random import randrange
    from time import sleep

    delays = [randrange(1, 10) for i in range(100)]

    def wait_delay(d):
        print 'sleeping for (%d)sec' % d
        sleep(d)

    pool = ThreadPool(20)

    for i, d in enumerate(delays):
        pool.add_task(wait_delay, d)

    sleep(100)
    delays = [randrange(10, 20) for i in range(100)]
    for i, d in enumerate(delays):
        pool.add_task(wait_delay, d)

    print 'wait completion'
    pool.wait_completion()

    for i, d in enumerate(delays):
        pool.add_task(wait_delay, d)
    print 'wait completion'
    pool.wait_completion()
