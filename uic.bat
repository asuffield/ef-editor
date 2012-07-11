@echo off

echo Running uic...
call c:\python27\Lib\site-packages\PyQt4\pyuic4 --from-imports -o ef\ui\fetch_wizard.py ui\fetch_wizard.ui
call c:\python27\Lib\site-packages\PyQt4\pyuic4 --from-imports -o ef\ui\editor.py ui\editor.ui
call c:\python27\Lib\site-packages\PyQt4\pyuic4 --from-imports -o ef\ui\upload_wizard.py ui\upload_wizard.ui
call c:\python27\Lib\site-packages\PyQt4\pyuic4 --from-imports -o ef\ui\membercheck.py ui\membercheck.ui
echo Done
