from urllib import urlencode
from PyQt4 import QtCore, QtNetwork
import os
import re
from types import StringType, UnicodeType

STRING_TYPES = StringType, UnicodeType

"""
Lifted this next couple of functions from mechanize - parses content-type headers

Utility functions for HTTP header value parsing and construction.

Copyright 1997-1998, Gisle Aas
Copyright 2002-2006, John J. Lee

This code is free software; you can redistribute it and/or modify it
under the terms of the BSD or ZPL 2.1 licenses (see the file
COPYING.txt included with the distribution).

"""

def unmatched(match):
    """Return unmatched part of re.Match object."""
    start, end = match.span(0)
    return match.string[:start]+match.string[end:]

token_re =        re.compile(r"^\s*([^=\s;,]+)")
quoted_value_re = re.compile(r"^\s*=\s*\"([^\"\\]*(?:\\.[^\"\\]*)*)\"")
value_re =        re.compile(r"^\s*=\s*([^\s;,]*)")
escape_re = re.compile(r"\\(.)")
def split_header_words(header_values):
    r"""Parse header values into a list of lists containing key,value pairs.

    The function knows how to deal with ",", ";" and "=" as well as quoted
    values after "=".  A list of space separated tokens are parsed as if they
    were separated by ";".

    If the header_values passed as argument contains multiple values, then they
    are treated as if they were a single value separated by comma ",".

    This means that this function is useful for parsing header fields that
    follow this syntax (BNF as from the HTTP/1.1 specification, but we relax
    the requirement for tokens).

      headers           = #header
      header            = (token | parameter) *( [";"] (token | parameter))

      token             = 1*<any CHAR except CTLs or separators>
      separators        = "(" | ")" | "<" | ">" | "@"
                        | "," | ";" | ":" | "\" | <">
                        | "/" | "[" | "]" | "?" | "="
                        | "{" | "}" | SP | HT

      quoted-string     = ( <"> *(qdtext | quoted-pair ) <"> )
      qdtext            = <any TEXT except <">>
      quoted-pair       = "\" CHAR

      parameter         = attribute "=" value
      attribute         = token
      value             = token | quoted-string

    Each header is represented by a list of key/value pairs.  The value for a
    simple token (not part of a parameter) is None.  Syntactically incorrect
    headers will not necessarily be parsed as you would want.

    This is easier to describe with some examples:

    >>> split_header_words(['foo="bar"; port="80,81"; discard, bar=baz'])
    [[('foo', 'bar'), ('port', '80,81'), ('discard', None)], [('bar', 'baz')]]
    >>> split_header_words(['text/html; charset="iso-8859-1"'])
    [[('text/html', None), ('charset', 'iso-8859-1')]]
    >>> split_header_words([r'Basic realm="\"foo\bar\""'])
    [[('Basic', None), ('realm', '"foobar"')]]

    """
    assert type(header_values) not in STRING_TYPES
    result = []
    for text in header_values:
        orig_text = text
        pairs = []
        while text:
            m = token_re.search(text)
            if m:
                text = unmatched(m)
                name = m.group(1)
                m = quoted_value_re.search(text)
                if m:  # quoted value
                    text = unmatched(m)
                    value = m.group(1)
                    value = escape_re.sub(r"\1", value)
                else:
                    m = value_re.search(text)
                    if m:  # unquoted value
                        text = unmatched(m)
                        value = m.group(1)
                        value = value.rstrip()
                    else:
                        # no value, a lone token
                        value = None
                pairs.append((name, value))
            elif text.lstrip().startswith(","):
                # concatenated headers, as per RFC 2616 section 4.2
                text = text.lstrip()[1:]
                if pairs: result.append(pairs)
                pairs = []
            else:
                # skip junk
                non_junk, nr_junk_chars = re.subn("^[=\s;]*", "", text)
                assert nr_junk_chars > 0, (
                    "split_header_words bug: '%s', '%s', %s" %
                    (orig_text, text, pairs))
                text = non_junk
        if pairs: result.append(pairs)
    return result

def encode_form_fields(fields):
    return urlencode(dict([(k,unicode(v).encode('utf-8')) for k,v in fields.items()]))

manager = None

def start_network_manager():
    global manager
    manager = QtNetwork.QNetworkAccessManager()

def stop_network_manager():
    global manager
    manager = None

def qt_form_post(url, fields, file=None):
    request = QtNetwork.QNetworkRequest(QtCore.QUrl(url))
    if file is None:
        request.setHeader(QtNetwork.QNetworkRequest.ContentTypeHeader, 'application/x-www-form-urlencoded; charset=utf-8')
        reply = manager.post(request, encode_form_fields(fields))
    else:
        multipart = QtNetwork.QHttpMultiPart(QtNetwork.QHttpMultiPart.FormDataType)
        for name, value in fields.iteritems():
            part = QtNetwork.QHttpPart()
            part.setHeader(QtNetwork.QNetworkRequest.ContentTypeHeader, 'text/plain; charset=utf-8')
            part.setHeader(QtNetwork.QNetworkRequest.ContentDispositionHeader, 'form-data; name="%s"' % name)
            part.setBody(value.encode('utf-8'))
            multipart.append(part)
        filepart = QtNetwork.QHttpPart()
        filepart.setHeader(QtNetwork.QNetworkRequest.ContentTypeHeader, file['type'])
        filepart.setHeader(QtNetwork.QNetworkRequest.ContentDispositionHeader, 'form-data; name="%s"; filename="%s"' % (file['name'], file['filename']))
        filepart.setBodyDevice(file['device'])
        multipart.append(filepart)
        reply = manager.post(request, multipart)
        # Hook multipart to the reply so that it sticks around for the lifetime of the request
        multipart.setParent(reply)
    return reply

def qt_page_get(url):
    request = QtNetwork.QNetworkRequest(QtCore.QUrl(url))
    reply = manager.get(request)
    return reply

def qt_reply_charset(reply):
    content_type = reply.header(QtNetwork.QNetworkRequest.ContentTypeHeader)
    if not content_type.isValid():
        return None
    charset = None
    for k, v in split_header_words([unicode(content_type.toString())])[0]:
        if k == 'charset':
            charset = v
    return charset

def qt_readall_charset(reply, charset):
    data = str(reply.readAll())
    if charset is not None:
        data = data.decode(charset)
    return data

def qt_relative_url(reply, url):
    relative_url = QtCore.QUrl(url)
    return reply.url().resolved(relative_url)
