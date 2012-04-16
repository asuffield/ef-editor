from ef.task import TaskOp, Task
from PyQt4 import QtCore, QtNetwork
from ef.netlib import split_header_words, qt_page_get, qt_form_post
from bs4 import BeautifulSoup

class NetworkError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

def qt_relative_url(reply, url):
    relative_url = QtCore.QUrl(url)
    return reply.url().resolved(relative_url)

class QNetworkReplyOp(TaskOp):
    def __init__(self, reply, redirecter=None):
        super(QNetworkReplyOp, self).__init__()

        self.reply = reply
        self.redirected_to = None
        self.reply.finished.connect(self.handle_finished)
        self.finish_processed = False

        if redirecter is not None:
            self.finished.connect(redirecter.finished)
            self.exception.connect(redirecter.exception)

    def resolve_url(self, relative_url):
        if self.redirected_to is not None:
            return self.redirected_to.resolve_url(relative_url)
        else:
            return self.reply.url().resolved(QtCore.QUrl(relative_url))

    def charset(self):
        content_type = self.reply.header(QtNetwork.QNetworkRequest.ContentTypeHeader)
        if not content_type.isValid():
            return None
        charset = None
        for k, v in split_header_words([unicode(content_type.toString())])[0]:
            if k == 'charset':
                return v
        return None

    def readall_charset(self):
        data = str(self.reply.readAll())
        charset = self.charset()
        if charset is not None:
            data = data.decode(charset)
        return data

    def result(self):
        if self.redirected_to is not None:
            return self.redirected_to.result()
        else:
            return self.readall_charset()

    def handle_finished(self):
        if self.finish_processed:
            return
        self.finish_processed = True

        if self.reply.error() != QtNetwork.QNetworkReply.NoError:
            self.exception.emit(NetworkError(self.reply.errorString()))
            return

        redirect = self.reply.attribute(QtNetwork.QNetworkRequest.RedirectionTargetAttribute)
        if redirect.isValid():
            url = self.resolve_url(redirect.toString())
            reply = self.reply.manager().get(QtNetwork.QNetworkRequest(url))
            self.redirected_to = QNetworkReplyOp(reply, self)
            return

        self.finished.emit()

    def abort(self):
        self.finish_processed = True
        if not self.reply.isFinished():
            self.reply.abort()
        if self.redirected_to is not None:
            self.redirected_to.abort()

class HTMLOp(QNetworkReplyOp):
    def result(self):
        data = super(HTMLOp, self).result()
        return BeautifulSoup(data, 'lxml')

class NetFuncs(object):
    def __init__(self):
        self.latest_net_op = None

    def _net_op(self, f, url, *args, **kwargs):
        if self.latest_net_op is not None:
            url = self.latest_net_op.resolve_url(url)
        self.latest_net_op = f(url, *args, **kwargs)
        return self.latest_net_op

    def get(self, url):
        return self._net_op(lambda url: HTMLOp(qt_page_get(url)), url)

    def get_raw(self, url):
        return self._net_op(lambda url: QNetworkReplyOp(qt_page_get(url)), url)

    def post(self, url, *args, **kwargs):
        return self._net_op(lambda url, *args, **kwargs: HTMLOp(qt_form_post(url, *args, **kwargs)), url, *args, **kwargs)

    def submit_form(self, form, user_fields={}, file=None):
        fields = {}
        action = form['action']

        for input in form.find_all('input'):
            if not input.has_key('name'):
                continue
            name = input['name']
            if input['type'] == 'image':
                fields['%s.x' % name] = '1'
                fields['%s.y' % name] = '1'
            elif input['type'] == 'button':
                continue
            elif input.has_key('value'):
                fields[name] = input['value']

        fields.update(user_fields)

        return self.post(action, fields, file)
