default:
	docker build -t calico/calico-dcos .

run: 
	docker run calico/calico-dcos
