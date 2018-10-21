NODES = 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18


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
	$(foreach node,$(NODES),mkdir -p nodes/n$(node)/local/;)
	$(foreach node,$(NODES),mkdir -p nodes/n$(node)/remote/;)

test_data:
	$(foreach node,$(NODES),cp ../data/n$(node)/* nodes/n$(node)/local/;)

clean:
	rm super_peer leaf_node
	rm -rf nodes/
	rm -rf logs/