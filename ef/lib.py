from PyQt4 import QtCore
from collections import OrderedDict

class LimitedSizeDict(OrderedDict):
  def __init__(self, *args, **kwds):
      self.size_limit = kwds.pop("size_limit", None)
      OrderedDict.__init__(self, *args, **kwds)
      self._check_size_limit()

  def __setitem__(self, key, value):
      if key in self:
          del self[key]
      OrderedDict.__setitem__(self, key, value)
      self._check_size_limit()

  def _check_size_limit(self):
      if self.size_limit is not None:
          while len(self) > self.size_limit:
              self.popitem(last=False)

class SignalGroup(QtCore.QObject):
    '''A SignalGroup will emit its fire signal once, after all the
    signals in the group have been emitted at least once. Repeat
    emissions and signal arguments are ignored'''
    
    fire = QtCore.pyqtSignal()
    def __init__(self, *signals):
        QtCore.QObject.__init__(self)
        self.pending = set()
        self.fired = False
        for signal in signals:
            self.pending.add(signal)
            signal.connect(lambda *args, **kwargs: self.handle_signal(signal))

    def handle_signal(self, signal):
        self.pending.discard(signal)
        if not self.fired and not self.pending:
            self.fire.emit()
            self.fired = True
