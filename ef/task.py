from PyQt4 import QtCore
import traceback
import sys

class TaskOp(QtCore.QObject):
    _finished = QtCore.pyqtSignal(int)
    _exception = QtCore.pyqtSignal(Exception, int, dict)
    
    def __init__(self):
        super(TaskOp, self).__init__()
        self.op_id = None
        self.task_done = False

    def set_op_id(self, op_id):
        self.op_id = op_id

    def get_op_id(self):
        return self.op_id

    @QtCore.pyqtSlot()
    def finish(self):
        # Suppress duplicate calls
        if self.task_done:
            return
        self._finished.emit(self.op_id)
        self.task_done = True

    def throw(self, e, msg=None, blob={}):
        self._exception.emit(e, self.op_id, blob)

    @QtCore.pyqtSlot(Exception, int)
    def rethrow(self, e, op_id):
        self.throw(e)

    def abort(self):
        pass

    def result(self):
        return None

    def emit_delayed(self):
        pass

class Finishable(object):
    def __init__(self, signal, error_signal=None):
        self.finish_signal = signal
        self.error_signal = error_signal
        self.finish_signal.connect(self.handle_finished_signal)
        if error_signal is not None:
            self.error_signal.connect(self.handle_error_signal)
        self.is_finished = False
        self.finished_error = None

    def handle_finished_signal(self):
        self.is_finished = True

    def handle_error_signal(self, *args):
        self.is_finished = True
        self.finished_error = args
    
class SignalWaitOp(TaskOp):
    def __init__(self, signal):
        TaskOp.__init__(self)

        self.signal = signal
        self.signal.connect(self.finish)

class FinishableWaitOp(TaskOp):
    def __init__(self, finishable):
        TaskOp.__init__(self)
        self.finishable = finishable
        if not self.finishable.is_finished:
            self.finishable.finish_signal.connect(self.finish)
            if self.finishable.error_signal is not None:
                self.finishable.error_signal.connect(self.throw)

    def emit_delayed(self):
        if self.finishable.finished_error is not None:
            self.throw(*self.finishable.finished_error)
        elif self.finishable.is_finished:
            self.finish()

class Task(QtCore.QObject, Finishable):
    task_finished = QtCore.pyqtSignal()
    task_aborted = QtCore.pyqtSignal()
    task_exception = QtCore.pyqtSignal(Exception, str, dict)

    # Coroutines are expected to access self.current for facilities of
    # the most recent operation to complete

    def __init__(self):
        QtCore.QObject.__init__(self)
        Finishable.__init__(self, self.task_finished, self.task_exception)

        self.task_coro = None
        self.current = None
        self.previous_op = None
        self.is_connected = False

        self.op_id = 1

    @QtCore.pyqtSlot()
    def start_task(self, *args, **kwargs):
        self.task_coro = self.task(*args, **kwargs)
        if self.task_coro is None:
            self.task_finished.emit()
        else:
            self.continue_task(lambda: self.task_coro.next())

    def connect_coro(self):
        if self.is_connected or self.current is None:
            return
        self.current.set_op_id(self.op_id)
        op_id = self.op_id = self.op_id + 1
        self.current._finished.connect(self.handle_finished)
        self.current._exception.connect(self.handle_exception)
        self.is_connected = True

    def disconnect_coro(self):
        if not self.is_connected or self.current is None:
            return
        self.current._finished.disconnect(self.handle_finished)
        self.current._exception.disconnect(self.handle_exception)
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
            # We have this mainly for FinishedWaitOp, which might have
            # finished immediately but needs to send its signals after
            # we connect to them
            self.current.emit_delayed()
        except StopIteration:
            self.task_finished.emit()
        except Exception, e:
            tb = sys.exc_info()[2]
            trace = traceback.format_exc()
            self.task_exception.emit(e, trace, {'traceback': tb})

    def handle_finished(self, op_id):
        if op_id != self.current.get_op_id():
            # Stray signal from a previous op
            return
        self.disconnect_coro()
        result = self.current.result()
        self.continue_task(lambda: self.task_coro.send(result))

    def handle_exception(self, e, op_id, blob):
        if op_id != self.current.get_op_id():
            # Stray signal from a previous op
            return

        self.disconnect_coro()

        # We've got an exception. We want to throw it through the
        # coroutine in order to get a useful traceback, and possibly
        # allow the coroutine to handle the exception
        self.continue_task(lambda: self.task_coro.throw(e, None, blob.get('traceback', None)))

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

class TaskList(Task):
    def __init__(self, *tasks):
        Task.__init__(self)

        self.task_list = []

        # This just flattens out iterables in the argument
        for task in tasks:
            try:
                self.task_list.extend(list(task))
            except TypeError:
                self.task_list.append(task)

    def task(self):
        for task in self.task_list:
            task.start_task()
            yield self.wait(task)
