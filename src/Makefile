all: super_peer leaf_node logging env_dirs test_data

super_peer: super_peer.cpp
	g++ super_peer.cpp -std=c++11 -pthread -o super_peer

leaf_node: leaf_node.cpp
	g++ leaf_node.cpp -std=c++11 -pthread -o leaf_node

logging:
	mkdir logs/
	mkdir logs/leaf_nodes/
	mkdir logs/super_peers/

env_dirs:
	mkdir nodes/
	mkdir nodes/n0/
	mkdir nodes/n1/
	mkdir nodes/n2/
	mkdir nodes/n3/
	mkdir nodes/n4/
	mkdir nodes/n5/
	mkdir nodes/n6/
	mkdir nodes/n7/
	mkdir nodes/n8/
	mkdir nodes/n9/
	mkdir nodes/n10/
	mkdir nodes/n11/
	mkdir nodes/n12/
	mkdir nodes/n13/
	mkdir nodes/n14/
	mkdir nodes/n15/
	mkdir nodes/n16/
	mkdir nodes/n17/
	mkdir nodes/n18/

test_data:
	cp ../data/n0/* nodes/n0/
	cp ../data/n1/* nodes/n1/
	cp ../data/n2/* nodes/n2/
	cp ../data/n3/* nodes/n3/
	cp ../data/n4/* nodes/n4/
	cp ../data/n5/* nodes/n5/
	cp ../data/n6/* nodes/n6/
	cp ../data/n7/* nodes/n7/
	cp ../data/n8/* nodes/n8/
	cp ../data/n9/* nodes/n9/
	cp ../data/n10/* nodes/n10/
	cp ../data/n11/* nodes/n11/
	cp ../data/n12/* nodes/n12/
	cp ../data/n13/* nodes/n13/
	cp ../data/n14/* nodes/n14/
	cp ../data/n15/* nodes/n15/
	cp ../data/n16/* nodes/n16/
	cp ../data/n17/* nodes/n17/
	cp ../data/n18/* nodes/n18/

clean:
	rm super_peer leaf_node
	rm -rf nodes/
	rm -rf logs/