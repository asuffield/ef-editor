from PyQt4 import QtCore
import traceback

class TaskOp(QtCore.QObject):
    finished = QtCore.pyqtSignal()
    exception = QtCore.pyqtSignal(Exception)
    
    def __init__(self):
        super(TaskOp, self).__init__()

    def abort(self):
        pass

    def result(self):
        return None

class Finishable(object):
    def __init__(self, signal):
        self.finish_signal = signal
        self.finish_signal.connect(self.handle_finished_signal)
        self.has_finished = False

    def handle_finished_signal(self):
        self.has_finished = True
    
    def is_finished(self):
        return self.has_finished

class SignalWaitOp(TaskOp):
    def __init__(self, signal):
        TaskOp.__init__(self)

        self.signal = signal
        self.signal.connect(self.finished)

class FinishableWaitOp(TaskOp):
    def __init__(self, finishable):
        TaskOp.__init__(self)
        self.finishable = finishable
        if self.finishable.is_finished():
            self.finished.emit()
        else:
            self.finishable.finish_signal.connect(self.finished)
    
class Task(QtCore.QObject):
    task_finished = QtCore.pyqtSignal()
    task_aborted = QtCore.pyqtSignal()
    task_exception = QtCore.pyqtSignal(Exception, str)

    # Coroutines are expected to access self.current for facilities of
    # the most recent operation to complete

    def __init__(self):
        super(Task, self).__init__()

        self.task_coro = None
        self.current = None
        self.previous_op = None
        self.is_connected = False

    @QtCore.pyqtSlot()
    def start_task(self, *args, **kwargs):
        self.task_coro = self.task(*args, **kwargs)
        self.continue_task(lambda: self.task_coro.next())

    def connect_coro(self):
        if self.is_connected or self.current is None:
            return
        self.current.finished.connect(self.handle_finished)
        self.current.exception.connect(self.handle_exception)
        self.is_connected = True

    def disconnect_coro(self):
        if not self.is_connected or self.current is None:
            return
        self.current.finished.disconnect(self.handle_finished)
        self.current.exception.disconnect(self.handle_exception)
        self.is_connected = False

    def continue_task(self, continuation):
        try:
            # This serves to make sure garbage collection is "one
            # iteration behind", so we never delete the Qt object
            # that's just emitted a 'finished' signal
            self.previous_op = self.current

            self.current = continuation()

            # Give the task one chance to respond (the only sensible response is to wrap the exception with more information and reraise)
            if not isinstance(self.current, TaskOp):
                self.current = self.task_coro.throw(TypeError("yield from task must be an instance of TaskOp"))

            # This should never happen, it means the task contains a hideous exception handling error
            if not isinstance(self.current, TaskOp):
                raise TypeError("yield from task must be an instance of TaskOp - double fault")

            self.connect_coro()
        except StopIteration:
            self.task_finished.emit()
        except Exception, e:
            self.task_exception.emit(e, traceback.format_exc())

    def handle_finished(self):
        self.disconnect_coro()
        result = self.current.result()
        self.continue_task(lambda: self.task_coro.send(result))

    def handle_exception(self, e):
        self.disconnect_coro()

        # We've got an exception. We want to throw it through the
        # coroutine in order to get a useful traceback, and possibly
        # allow the coroutine to handle the exception
        self.continue_task(lambda: self.task_coro.throw(e))

    def wait(self, until):
        if isinstance(until, Finishable):
            return FinishableWaitOp(until)
        else:
            return SignalWaitOp(until)

    @QtCore.pyqtSlot()
    def abort(self):
        self.disconnect_coro()
        if self.current is not None:
            self.current.abort()
        self.task_aborted.emit()
