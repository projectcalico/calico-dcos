default:
	docker build -t calico/calico-dcos -f Dockerfile.framework .

run: 
	docker run calico/calico-dcos

binary: dist/calico_installer

dist/calico_installer: $(CALICO_MESOS_FILES)
	mkdir -p -m 777 dist/
	# Build the mesos plugin
	docker build -t calico/dcos-builder -f Dockerfile.installer .
	docker run --rm \
         -v `pwd`/installer/:/code/installer \
         -v `pwd`/dist/:/code/dist \
	     calico/dcos-builder \
	     pyinstaller installer/installer.py -ayF
