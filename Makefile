default:
	docker build -t calico/calico-dcos -f framework.Dockerfile .

run: 
	docker run calico/calico-dcos
binary: dist/calico_installer
dist/calico_installer: $(CALICO_MESOS_FILES)
	mkdir -p -m 777 dist/
	# Build the mesos plugin
	docker build -t calico/dcos-builder -f installer.Dockerfile .
	docker run --rm \
         -v `pwd`/calico_dcos/:/code/calico_dcos \
         -v `pwd`/dist/:/code/dist \
	 calico/dcos-builder \
	 pyinstaller calico_dcos/agent/installer.py -ayF
