from distutils.core import setup
import py2exe

setup(
    version = "0.9.18",
    description = "Image editor for eventsforce",
    name = "ef-image-editor",

    options = {
        'py2exe': {
            'includes': ['sip', 'PyQt4.QtSql', 'lxml.etree', 'lxml._elementpath', 'gzip'],
            'packages': ['sqlalchemy'],
            'dll_excludes': ['MSVCP90.dll'],
            },
        },

    data_files = [
        ('sqldrivers', [r'c:\Python27\Lib\site-packages\PyQt4\plugins\sqldrivers\qsqlite4.dll']),
        ('imageformats', [r'c:\Python27\Lib\site-packages\PyQt4\plugins\imageformats\qjpeg4.dll',
                          r'c:\Python27\Lib\site-packages\PyQt4\plugins\imageformats\qgif4.dll',
                          r'c:\Python27\Lib\site-packages\PyQt4\plugins\imageformats\qmng4.dll',
                          ]),
        ],

    # targets to build
    windows = ["ef-image-editor.py", 'membercheck.py', 'duplicatedetect.py'],
    )
