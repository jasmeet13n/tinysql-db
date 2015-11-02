cc = g++

tokenizer.o: tokenizer.cc
	$(cc) -c tokenizer.cc

tokenizer_test.o: tokenizer_test.cc
	$(cc) -c tokenizer_test.cc

tokenizer_test: tokenizer.o tokenizer_test.o
	$(cc)   -o a.out tokenizer.o tokenizer_test.o -lgtest -lpthread	
	
clean:
	rm *.o
	rm a.out