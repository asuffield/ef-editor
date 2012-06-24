uis = ui/editor.ui ui/fetch_wizard.ui ui/upload_wizard.ui ui/membercheck.ui ui/duplicatedetect.ui
ui_pys = $(patsubst ui/%.ui,ef/ui/%.py,$(uis))

all: $(ui_pys)

ef/ui/resources_rc.py: resources.qrc
	pyrcc4 -o ef/ui/resources_rc.py resources.qrc

ef/ui/%.py: ui/%.ui
	pyuic4 --from-imports -o $@ $<
