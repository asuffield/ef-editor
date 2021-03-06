from ef.task import TaskOp, Task
from PyQt4 import QtCore, QtNetwork
from ef.netlib import split_header_words, qt_page_get, qt_form_post
from bs4 import BeautifulSoup
import re

class NetworkError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return self.value

class NetworkTimeout(NetworkError):
    def __init__(self, url):
        NetworkError.__init__(self, 'Network operation timed out for %s' % url.toEncoded())

class QNetworkReplyOp(TaskOp):
    def __init__(self, reply, timeout=None, redirecter=None):
        super(QNetworkReplyOp, self).__init__()

        self.reply = reply
        if timeout is not None:
            self.timer = QtCore.QTimer(self)
            self.timer.setInterval(timeout * 1000)
            self.timer.setSingleShot(True)
            self.timer.timeout.connect(self.handle_timeout)
            self.timer.start()
        self.redirected_to = None
        self.reply.finished.connect(self.handle_finished)
        self.finish_processed = False

        if redirecter is not None:
            self._finished.connect(redirecter.finish)
            self._exception.connect(redirecter.rethrow)

    def __str__(self):
        return 'QNetworkReply(%s)' % self.reply.url().toEncoded()

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
            self.throw(NetworkError(self.reply.errorString()))
            return

        redirect = self.reply.attribute(QtNetwork.QNetworkRequest.RedirectionTargetAttribute)
        if redirect.isValid():
            url = self.resolve_url(redirect.toString())
            reply = self.reply.manager().get(QtNetwork.QNetworkRequest(url))
            # Note that redirects will be timed out by the calling
            # class, which will abort the whole chain. This handles
            # loops neatly.
            self.redirected_to = QNetworkReplyOp(reply, None, redirecter=self)
            return

        self.finish()

    def handle_timeout(self):
        if self.finish_processed:
            return
        self.throw(NetworkTimeout(self.reply.url()))

    def abort(self):
        self.finish_processed = True
        if not self.reply.isFinished():
            self.reply.abort()
        if self.redirected_to is not None:
            self.redirected_to.abort()

class HTMLOp(QNetworkReplyOp):
    def __init__(self, *args, **kwargs):
        self.parse_only = kwargs.pop('parse_only', None)
        QNetworkReplyOp.__init__(self, *args, **kwargs)
    
    def result(self):
        data = super(HTMLOp, self).result()
        soup = BeautifulSoup(data, 'lxml', parse_only=self.parse_only)
        return soup

class NetFuncs(object):
    def __init__(self):
        self.latest_net_op = None

    def _net_op(self, f, url, *args, **kwargs):
        if self.latest_net_op is not None:
            url = self.latest_net_op.resolve_url(url)
        self.latest_net_op = f(url, *args, **kwargs)
        return self.latest_net_op

    def get(self, url, **kwargs):
        return self._net_op(lambda url: HTMLOp(qt_page_get(url), **kwargs), url)

    def get_raw(self, url, timeout=30):
        return self._net_op(lambda url: QNetworkReplyOp(qt_page_get(url), timeout=timeout), url)

    def post(self, url, *args, **kwargs):
        timeout = kwargs.pop('timeout', 30)
        parse_only = kwargs.pop('parse_only', None)
        return self._net_op(lambda url, *args, **kwargs: HTMLOp(qt_form_post(url, *args, **kwargs), timeout=timeout, parse_only=parse_only), url, *args, **kwargs)

    def submit_form(self, form, user_fields={}, file=None, timeout=30, parse_only=None, default_fields={}):
        fields = {}
        fields_seen = set()
        action = form['action']

        for input in form.find_all('input'):
            if not input.has_key('name'):
                continue
            name = input['name']
            if input.has_key('disabled'):
                print "Skipping disabled select", name
                continue
            fields_seen.add(name)
            type = input['type'].lower()
            if type == 'image':
                fields['%s.x' % name] = '1'
                fields['%s.y' % name] = '1'
            elif type == 'button':
                continue
            elif type == 'checkbox' or type == 'radio':
                if input.has_key('checked'):
                    fields[name] = input['value']
            elif input.has_key('value'):
                fields[name] = input['value']
                #if input['value'] == 'Northern Ireland':
                #    f = open('tmp.html', 'w')
                #    soup = list(form.parents)[-2]
                #    print soup
                #    f.write(str(soup))
                #    f.close()

        for select in form.find_all('select'):
            if not select.has_key('name'):
                continue
            name = select['name']
            if select.has_key('disabled'):
                print "Skipping disabled select", name
                continue
            fields_seen.add(name)
            selected = filter(lambda o: o.has_key('selected'), select.find_all('option'))
            if len(selected):
                fields[name] = selected[0]['value']

        fields_missed = fields_seen - set(fields.keys())

        for rexp in default_fields:
            for name in fields_missed:
                if rexp.match(name):
                    fields[name] = default_fields[rexp]

        # Hideous eventsforce hack: they do this field-disabling mess in javascript
        for script in form.find_all('script'):
            m = re.search(r"depends = depends \+ '(.*)'", script.text)
            if m:
                depend_str = m.group(1)
                m = re.match(r'^(.*)->(.*)->(.*)\|$', depend_str)
                child_field = m.group(1)
                parent_field = m.group(2)
                parent_value = m.group(3)
                if fields.has_key(child_field):
                    if not fields.has_key(parent_field) or fields[parent_field] != parent_value:
                        del fields[child_field]

        fields.update(user_fields)
        #print [action, fields]

        return self.post(action, fields, file, timeout=timeout, parse_only=parse_only)
