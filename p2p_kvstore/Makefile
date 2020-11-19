.PHONY: word linus demo serial map

build:
	g++ -pthread -g -std=c++11 -o dataf test/test_dataframe.cpp
	g++ -pthread -g -std=c++11 -o serial test/test_serialization.cpp
	g++ -pthread -g -std=c++11 -o map test/test_map.cpp
	g++ -pthread -g -std=c++11 -o kvstore test/test_kvstore.cpp
	g++ -pthread -g -std=c++11 -o lmap test/test_local_map.cpp
	g++ -pthread -g -std=c++11 -o trivial test/trivial.cpp
	g++ -pthread -g -std=c++11 -o demo test/demo.cpp
	g++ -pthread -g -std=c++11 -o word test/word_count.cpp
	g++ -pthread -g -std=c++11 -o linus test/linus.cpp
	wget https://raw.githubusercontent.com/spencerlachance/cs4500datafile/master/datafile.zip
	mv datafile.zip data
	unzip data/datafile.zip -d data

run:
	./dataf -f data/datafile.txt -len 1000000
	./serial
	./map
	./kvstore
	./lmap -i 0 &
	./lmap -i 1 &
	./lmap -i 2
	./trivial
	./demo -idx 1 &
	./demo -idx 2 &
	./demo -idx 0
	./word -i 1 -n 3 -f data/100k.txt &
	./word -i 2 -n 3 -f data/100k.txt &
	./word -i 0 -n 3 -f data/100k.txt
	./linus -i 1 -n 4 &
	./linus -i 2 -n 4 &
	./linus -i 3 -n 4 &
	./linus -i 0 -n 4 -l 1000000

valgrind:
	valgrind --leak-check=full ./dataf -f data/datafile.txt -len 100000
	valgrind --leak-check=full ./serial
	valgrind --leak-check=full ./map
	valgrind --leak-check=full ./kvstore
	./lmap -i 1 &
	./lmap -i 2 &
	valgrind --leak-check=full ./lmap -i 0
	valgrind --leak-check=full ./trivial -v
	valgrind --leak-check=full ./demo -v -idx 1 &
	valgrind --leak-check=full ./demo -v -idx 2 &
	valgrind --leak-check=full ./demo -v -idx 0
	valgrind --leak-check=full ./word -i 1 -n 2 -f data/100k.txt &
	valgrind --leak-check=full ./word -i 0 -n 2 -f data/100k.txt
	valgrind --leak-check=full ./linus -i 1 -n 2 &
	valgrind --leak-check=full ./linus -i 0 -n 2 -l 100000

clean:
	rm dataf serial map kvstore lmap trivial demo word linus data/datafile.*

df:
	g++ -pthread -g -std=c++11 -o dataf test/test_dataframe.cpp
	wget https://raw.githubusercontent.com/spencerlachance/cs4500datafile/master/datafile.zip
	mv datafile.zip data
	unzip data/datafile.zip -d data
	./dataf -f data/datafile.txt -len 1000000
	rm dataf data/datafile.*

serial:
	g++ -pthread -g -std=c++11 -o serial test/test_serialization.cpp
	./serial
	rm serial

map:
	g++ -pthread -g -std=c++11 -o map test/test_map.cpp
	./map
	rm map

kv:
	g++ -pthread -g -std=c++11 -o kvstore test/test_kvstore.cpp
	./kvstore
	rm kvstore

lm:
	g++ -pthread -g -std=c++11 -o lmap test/test_local_map.cpp
	./lmap -i 0 &
	./lmap -i 1 &
	./lmap -i 2
	rm lmap

demo:
	g++ -pthread -g -std=c++11 -o demo test/demo.cpp
	./demo -idx 1 &
	./demo -idx 2 &
	./demo -idx 0
	rm demo

word:
	g++ -pthread -g -std=c++11 -o word test/word_count.cpp
	./word -i 0 -n 3 -f data/100k.txt &
	./word -i 1 -n 3 -f data/100k.txt &
	./word -i 2 -n 3 -f data/100k.txt
	rm word

linus:
	g++ -pthread -g -std=c++11 -o linus test/linus.cpp
	./linus -i 1 -n 4 &
	./linus -i 2 -n 4 &
	./linus -i 3 -n 4 &
	./linus -i 0 -n 4 -l 1000000
	rm linus