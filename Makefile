uis = ui/editor.ui ui/fetch_wizard.ui
ui_pys = $(patsubst ui/%.ui,ef/ui/%.py,$(uis))

all: $(ui_pys)

ef/ui/%.py: ui/%.ui
	pyuic4 -o $@ $<
