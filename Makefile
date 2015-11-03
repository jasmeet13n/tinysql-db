cc = g++ -std=c++11

# Tokenizer
tokenizer.o: tokenizer.cc
	$(cc) -c tokenizer.cc

tokenizer_test.o: tokenizer_test.cc
	$(cc) -c tokenizer_test.cc

tokenizer_test: tokenizer.o tokenizer_test.o
	$(cc)   -o a.out tokenizer.o tokenizer_test.o -lgtest -lpthread
	
	
# Parser	
parser.o: parser.cc
	$(cc) -c parser.cc

parser_test.o: parser_test.cc
	$(cc) -c parser_test.cc

parser_test: parser.o parser_test.o
	$(cc)   -o a.out parser.o parser_test.o -lgtest -lpthread	
	
clean:
	rm *.o
	rm a.out